#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include "infelf_rdma.h"
#include "infelf_gpu_mem.h"
#include "example.h"

int server_recv(struct infelf_ctrl_t *cb, struct ibv_wc *wc)
{
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    if (wc->byte_len != sizeof(cb->recv_buf)) {
        ERROR("server: received bogus data, size %d\n", wc->byte_len);
        return -1;
    }

    cb->remote_rkey = be32toh(cb->recv_buf.rkey);
    cb->remote_addr = be64toh(cb->recv_buf.buf);
    cb->remote_len  = be32toh(cb->recv_buf.size);
    DEBUG("server: received rkey %x addr %" PRIx64 " len %d from peer\n",
          cb->remote_rkey, cb->remote_addr, cb->remote_len);

    if (app->state <= CONNECTED || app->state == RDMA_WRITE_COMPLETE)
        app->state = RDMA_READ_ADV;
    else
        app->state = RDMA_WRITE_ADV;

    return 0;
}

static int example_accept(struct infelf_ctrl_t *cb)
{
    int ret;

    DEBUG("server: accepting client connection request\n");

    ret = rdma_accept(cb->child_cm_id, NULL);
    if (ret) {
        ERROR("server: rdma_accept() failed\n");
        return ret;
    }

    /* sem_wait(&app->sem); */
    /* if (app->state == ERROR) { */
    /*     fprintf(stderr, "wait for CONNECTED state %d\n", app->state); */
    /*     return -1; */
    /* } */
    return 0;
}

static int example_test_server(struct infelf_ctrl_t *cb)
{
    struct ibv_send_wr *bad_wr;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int ret;

    while (1) {
        /* Wait for client's Start STAG/TO/Len */
        while (app->state != RDMA_READ_ADV) {
            sleep_us(1000000);
        }
        DEBUG("server received sink adv\n");

        INFO("server: TEST CASE 1: Issue rdma_read() to client\n");
        cb->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
        cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
        cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
        cb->rdma_sq_wr.sg_list->length = cb->remote_len;

        ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
        if (ret) {
            ERROR("server: post send error %d\n", ret);
            break;
        }
        DEBUG("server: posted rdma read req \n");

        /* Wait to confirm the read completion */
        DEBUG("server: waiting for RDMA_READ_COMPLETE state %d\n", app->state);
        while (app->state != RDMA_READ_COMPLETE) {
            sleep_us(1000000);
        }
        DEBUG("server: received read complete\n");

        /* Display data in recv buf */
        if (app->verbose)
            printf("rdma_read() to client: %s\n", cb->rdma_buf);

        INFO("server: TEST CASE 2: Issue rdma_write() to client\n");
        /* Tell client to continue */
        ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
        if (ret) {
            ERROR("server: post send error %d\n", ret);
            break;
        }
        DEBUG("server: posted go ahead\n");

        /* Wait for client's RDMA STAG/TO/Len */
        DEBUG("server: waiting for RDMA_WRITE_ADV state %d\n", app->state);
        while (app->state != RDMA_WRITE_ADV) {
            sleep_us(1000000);
        }
        DEBUG("server: server received sink adv\n");

        /* RDMA Write echo data */
        cb->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
        cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
        cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
        cb->rdma_sq_wr.sg_list->length = strlen(cb->rdma_buf) + 1;
        DEBUG("server: rdma write from lkey %x laddr %" PRIx64 " len %d\n",
              cb->rdma_sq_wr.sg_list->lkey,
              cb->rdma_sq_wr.sg_list->addr,
              cb->rdma_sq_wr.sg_list->length);

        ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
        if (ret) {
            ERROR("server: post send error %d\n", ret);
            break;
        }

        /* Wait to confirm the write completion */
        DEBUG("server: waiting for RDMA_WRITE_COMPLETE state %d\n", app->state);
        while (app->state != RDMA_WRITE_COMPLETE) {
            sleep_us(1000000);
        }
        DEBUG("server: rdma write complete \n");

        /* Tell client to begin again */
        ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
        if (ret) {
            ERROR("server: post send error %d\n", ret);
            break;
        }
        DEBUG("server: posted go ahead\n");
    }

    return (app->state == DISCONNECTED) ? 0 : ret;
}

static int example_bind_server(struct infelf_ctrl_t *cb)
{
    int ret;

    if (cb->sin.ss_family == AF_INET)
        ((struct sockaddr_in *) &cb->sin)->sin_port = cb->port;
    else
        ((struct sockaddr_in6 *) &cb->sin)->sin6_port = cb->port;

    ret = rdma_bind_addr(cb->cm_id, (struct sockaddr *) &cb->sin);
    if (ret) {
        ERROR("server: rdma_bind_addr() failed");
        return ret;
    }
    DEBUG("rdma_bind_addr() successful\n");

    ret = rdma_listen(cb->cm_id, 3);
    if (ret) {
        ERROR("server: rdma_listen() failed");
        return ret;
    }
    DEBUG("server: rdma_listen() successful\n");

    return 0;
}

int run_server(struct infelf_ctrl_t *cb)
{
    struct ibv_recv_wr *bad_wr;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int ret;

    ret = example_bind_server(cb);
    if (ret)
        return ret;

    DEBUG("server: waiting for CONNECT_REQUEST state %d\n", app->state);
    while (app->state != CONNECT_REQUEST) {
        sleep_us(1000000);
    }

    ret = infelf_setup_qp(cb, cb->child_cm_id);
    if (ret) {
        ERROR("server: setup_qp failed: %d\n", ret);
        return ret;
    }

    /* Always allocate CPU memory on server side */
    ret = infelf_setup_buffers(cb, -1, app->size);
    if (ret) {
        ERROR("server: infelf_setup_buffers failed: %d\n", ret);
        goto err1;
    }

    ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
    if (ret) {
        ERROR("server: ibv_post_recv failed: %d\n", ret);
        goto err2;
    }

    ret = pthread_create(&app->cqthread, NULL, cq_thread, cb);
    if (ret) {
        ERROR("server: pthread_create() failed\n");
        goto err2;
    }

    ret = example_accept(cb);
    if (ret) {
        ERROR("server: connect error %d\n", ret);
        goto err2;
    }

    ret = example_test_server(cb);
    if (ret) {
        ERROR("server: failed with ret %d\n", ret);
        goto err3;
    }

    ret = 0;

err3:
    infelf_disconnect(cb->child_cm_id);
    pthread_join(app->cqthread, NULL);
    rdma_destroy_id(cb->child_cm_id);
err2:
    infelf_free_buffers(cb);
err1:
    infelf_free_qp(cb);

    return 0;
}
