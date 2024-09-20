#include <stdio.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include "infelf_rdma.h"
#include "infelf_gpu_mem.h"
#include "example.h"

#ifdef HAVE_CUDA
#include "cuda_runtime.h"
#endif


int client_recv(struct infelf_ctrl_t *cb, struct ibv_wc *wc)
{
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;

    if (wc->byte_len != sizeof(cb->recv_buf)) {
        ERROR("client: received bogus data, size %d\n", wc->byte_len);
        return -1;
    }

    if (app->state == RDMA_READ_ADV)
        app->state = RDMA_WRITE_ADV;
    else
        app->state = RDMA_WRITE_COMPLETE;

    return 0;
}

static void fill_buffer(char *buf, int size) {
    int start, cc, i;
    unsigned char c;

    start = 65;
    cc = snprintf(buf, size, INFELF_TEST_MSG_FMT);
    for (i = cc, c = start; i < size; i++) {
        buf[i] = c;
        c++;
        if (c > 122)
            c = 65;
    }
    start++;
    if (start > 122)
        start = 65;
    buf[size - 1] = 0;
}

static int example_test_client(struct infelf_ctrl_t *cb)
{
    int ret = 0;
    struct ibv_send_wr *bad_wr;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;

    INFO("client: TEST CASE 1: Prepare for server to rdma_read()\n");
    app->state = RDMA_READ_ADV;
    /* Put some ascii text in the buffer. */

    if (cb->gpu_bdf) {
#ifdef HAVE_CUDA
        char *tmp_buf = malloc(app->size);
        if (!tmp_buf) {
            return -1;
        }
        fill_buffer(tmp_buf, app->size);
        cudaMemcpy(cb->start_buf, tmp_buf, app->size, cudaMemcpyHostToDevice);
        free(tmp_buf);
#else
        ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
        return -1;
#endif
    } else {
        fill_buffer(cb->start_buf, app->size);
    }

    /* Send the start_buf/start_mr for server to do rdma_read() */
    infelf_format_send(cb, cb->start_buf, cb->start_mr);
    ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
    if (ret) {
        ERROR("client: post send error %d\n", ret);
        return -1;
    }
    INFO("client: ready for rdma_read()\n");

    INFO("client: TEST CASE 2: Print contents of rdma_write() from server\n");
    /* Wait to confirm the server is done with 1st test case */
    DEBUG("client: waiting for RDMA_WRITE_ADV state %d\n", app->state);
    while (app->state != RDMA_WRITE_ADV) {
        sleep_us(1000000);
    }

    /* Prepare the memory to be used for rdma_write() */
    infelf_format_send(cb, cb->rdma_buf, cb->rdma_mr);
    ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
    if (ret) {
        ERROR("post send error %d\n", ret);
        return -1;
    }

    /* Wait to confirm the server is done with rdma_write() */
    DEBUG("client: waiting for RDMA_WRITE_COMPLETE state %d\n", app->state);
    while (app->state != RDMA_WRITE_COMPLETE) {
        sleep_us(1000000);
    }

    if (app->verbose) {
        if (cb->gpu_bdf) {
#ifdef HAVE_CUDA
            char *tmp_buf = malloc(app->size);
            if (!tmp_buf) {
                return -1;
            }
            cudaMemcpy(tmp_buf, cb->rdma_buf, app->size, cudaMemcpyDeviceToHost);
            printf("rdma_write() from server (GPU): %s\n", tmp_buf);
            free(tmp_buf);
#else
            ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
            return -1;
#endif
        } else {
            printf("rdma_write() from server (CPU): %s\n", cb->rdma_buf);
        }
    }

    return (app->state == DISCONNECTED) ? 0 : ret;
}

static int example_connect_client(struct infelf_ctrl_t *cb)
{
    struct rdma_conn_param conn_param;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int ret;

    infelf_init_conn_param(&conn_param);
    ret = rdma_connect(cb->cm_id, &conn_param);
    if (ret) {
        ERROR("client: rdma_connect() failed\n");
        return ret;
    }

    while (app->state != CONNECTED) {
        sleep_us(1000000);
    }
    DEBUG("client: rdma_connect successful\n");
    return 0;
}

static int example_bind_client(struct infelf_ctrl_t *cb)
{
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int ret;

    if (cb->sin.ss_family == AF_INET)
        ((struct sockaddr_in *) &cb->sin)->sin_port = cb->port;
    else
        ((struct sockaddr_in6 *) &cb->sin)->sin6_port = cb->port;

    if (cb->ssource.ss_family) 
        ret = rdma_resolve_addr(cb->cm_id, (struct sockaddr *) &cb->ssource,
                                (struct sockaddr *) &cb->sin, 2000);
    else
        ret = rdma_resolve_addr(cb->cm_id, NULL, (struct sockaddr *) &cb->sin, 2000);

    if (ret) {
        ERROR("client: rdma_resolve_addr() failed\n");
        return ret;
    }

    while (app->state != ROUTE_RESOLVED) {
        sleep_us(1000000);
    }
    DEBUG("client: rdma_resolve_route successful\n");
    return 0;
}

int run_client(struct infelf_ctrl_t *cb)
{
    struct ibv_recv_wr *bad_wr;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int dev_id = -1, ret;

    ret = example_bind_client(cb);
    if (ret)
        return ret;

    ret = infelf_setup_qp(cb, cb->cm_id);
    if (ret) {
        ERROR("client: setup_qp failed: %d\n", ret);
        return ret;
    }

    if (cb->gpu_bdf) {
        dev_id = infelf_gpu_init(cb->gpu_bdf);
    }

    ret = infelf_setup_buffers(cb, dev_id, app->size);
    if (ret) {
        ERROR("client: infelf_setup_buffers failed: %d\n", ret);
        goto err1;
    }

    ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
    if (ret) {
        ERROR("client: ibv_post_recv failed: %d\n", ret);
        goto err2;
    }

    ret = pthread_create(&app->cqthread, NULL, cq_thread, cb);
    if (ret) {
        ERROR("client: pthread_create() failed\n");
        goto err2;
    }

    ret = example_connect_client(cb);
    if (ret) {
        ERROR("client: connect error %d\n", ret);
        goto err3;
    }

    ret = example_test_client(cb);
    if (ret) {
        ERROR("client: failed with ret: %d\n", ret);
        goto err4;
    }

    if (cb->gpu_bdf) {
       infelf_gpu_close();
    }

    ret = 0;
err4:
    infelf_disconnect(cb->cm_id);
err3:
    pthread_join(app->cqthread, NULL);
err2:
    infelf_free_buffers(cb);
err1:
    infelf_free_qp(cb);

    return ret;
}

