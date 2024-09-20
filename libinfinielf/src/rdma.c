#include <stdio.h>
#include <stdlib.h>
#include <rdma/rdma_cma.h>
#include "infelf_rdma.h"
#include "infelf_gpu_mem.h"

struct rdma_event_channel* infelf_create_event_channel(void)
{
    struct rdma_event_channel *channel;

    channel = rdma_create_event_channel();
    if (!channel) {
        if (errno == ENODEV)
            ERROR("No RDMA devices were detected\n");
        else
            ERROR("Failed to create RDMA CM event channel\n");
    }
    return channel;
}

void infelf_init_conn_param(struct rdma_conn_param *conn_param)
{
    memset(conn_param, 0, sizeof(*conn_param));
    conn_param->responder_resources = 1;
    conn_param->initiator_depth = 1;
    conn_param->retry_count = 7;
    conn_param->rnr_retry_count = 7;
}

int infelf_disconnect(struct rdma_cm_id *id)
{
    return rdma_disconnect(id);
}

static void infelf_setup_wr(struct infelf_ctrl_t *cb)
{
    cb->recv_sgl.addr = (uint64_t) (unsigned long) &cb->recv_buf;
    cb->recv_sgl.length = sizeof cb->recv_buf;
    cb->recv_sgl.lkey = cb->recv_mr->lkey;
    cb->rq_wr.sg_list = &cb->recv_sgl;
    cb->rq_wr.num_sge = 1;

    cb->send_sgl.addr = (uint64_t) (unsigned long) &cb->send_buf;
    cb->send_sgl.length = sizeof cb->send_buf;
    cb->send_sgl.lkey = cb->send_mr->lkey;

    cb->sq_wr.opcode = IBV_WR_SEND;
    cb->sq_wr.send_flags = IBV_SEND_SIGNALED;
    cb->sq_wr.sg_list = &cb->send_sgl;
    cb->sq_wr.num_sge = 1;

    cb->rdma_sgl.addr = (uint64_t) (unsigned long) cb->rdma_buf;
    cb->rdma_sgl.lkey = cb->rdma_mr->lkey;
    cb->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
    cb->rdma_sq_wr.sg_list = &cb->rdma_sgl;
    cb->rdma_sq_wr.num_sge = 1;
}

int infelf_setup_buffers(struct infelf_ctrl_t *cb, int gpu_dev_id, int size)
{
    int ret;

    DEBUG("infelf_setup_buffers called on cb %p\n", cb);

    cb->buf_size = size;

    cb->recv_mr = ibv_reg_mr(cb->pd, &cb->recv_buf, sizeof cb->recv_buf,
                             IBV_ACCESS_LOCAL_WRITE);
    if (!cb->recv_mr) {
        ERROR("recv_buf reg_mr failed\n");
        return errno;
    }

    cb->send_mr = ibv_reg_mr(cb->pd, &cb->send_buf, sizeof cb->send_buf, 0);
    if (!cb->send_mr) {
        ERROR("send_buf reg_mr failed\n");
        ret = errno;
        goto err1;
    }

    /* TODO: Use memalign also for buffers on CPU
     *  int page_size = sysconf(_SC_PAGESIZE);
     *  buff = memalign(page_size, length);
     */
    /* Allocate buffer from GPU if gpu_dev_id >= 0 */
    cb->rdma_buf = (gpu_dev_id >= 0) ?
        infelf_gpu_buffer_alloc(size, gpu_dev_id) : malloc(size);
    if (!cb->rdma_buf) {
        ERROR("rdma_buf malloc failed on %s\n",
              (gpu_dev_id >= 0) ? "GPU" : "CPU");
        ret = -ENOMEM;
        goto err2;
    }

    cb->rdma_mr = ibv_reg_mr(cb->pd, cb->rdma_buf, size,
                             IBV_ACCESS_LOCAL_WRITE |
                             IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE);
    if (!cb->rdma_mr) {
        ERROR("rdma_buf reg_mr failed\n");
        ret = errno;
        goto err3;
    }

    if (!cb->server) {
        cb->start_buf = (gpu_dev_id >= 0) ?
            infelf_gpu_buffer_alloc(size, gpu_dev_id) : malloc(size);
        if (!cb->start_buf) {
            ERROR("start_buf allocation failed on %s\n",
                  (gpu_dev_id >= 0) ? "GPU" : "CPU");
            ret = -ENOMEM;
            goto err4;
        }

        cb->start_mr = ibv_reg_mr(cb->pd, cb->start_buf, size,
                                  IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_WRITE);
        if (!cb->start_mr) {
            ERROR("start_buf ibv_reg_mr() failed\n");
            ret = errno;
            goto err5;
        }
    }

    infelf_setup_wr(cb);
    DEBUG("allocated & registered buffers successful.\n");
    return 0;

err5:
    free(cb->start_buf);
err4:
    ibv_dereg_mr(cb->rdma_mr);
err3:
    free(cb->rdma_buf);
err2:
    ibv_dereg_mr(cb->send_mr);
err1:
    ibv_dereg_mr(cb->recv_mr);
    return ret;
}

void infelf_free_buffers(struct infelf_ctrl_t *cb)
{
    ibv_dereg_mr(cb->recv_mr);
    ibv_dereg_mr(cb->send_mr);
    ibv_dereg_mr(cb->rdma_mr);
    if (cb->gpu_bdf) {
        infelf_gpu_buffer_free(cb->rdma_buf);
    } else {
        free(cb->rdma_buf);
    }
    if (!cb->server) {
        ibv_dereg_mr(cb->start_mr);
        if (cb->gpu_bdf) {
            infelf_gpu_buffer_free(cb->start_buf);
        } else {
            free(cb->start_buf);
        }
    }
    DEBUG("buffers freed on cb %p\n", cb);
}

static int infelf_create_qp(struct infelf_ctrl_t *cb)
{
    struct ibv_qp_init_attr init_attr;
    struct rdma_cm_id *id;
    int ret;

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = INFELF_SQ_DEPTH;
    init_attr.cap.max_recv_wr = 2;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_send_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cb->cq;
    init_attr.recv_cq = cb->cq;
    id = cb->server ? cb->child_cm_id : cb->cm_id;

    ret = rdma_create_qp(id, cb->pd, &init_attr);
    if (!ret)
        cb->qp = id->qp;
    else
        ERROR("rdma_create_qp() failed");
    return ret;
}

void infelf_free_qp(struct infelf_ctrl_t *cb)
{
    ibv_destroy_qp(cb->qp);
    ibv_destroy_cq(cb->cq);
    ibv_destroy_comp_channel(cb->channel);
    ibv_dealloc_pd(cb->pd);
}

int infelf_setup_qp(struct infelf_ctrl_t *cb, struct rdma_cm_id *cm_id)
{
    int ret;

    cb->pd = ibv_alloc_pd(cm_id->verbs);
    if (!cb->pd) {
        ERROR("ibv_alloc_pd failed\n");
        return errno;
    }
    DEBUG("created pd %p\n", cb->pd);

    cb->channel = ibv_create_comp_channel(cm_id->verbs);
    if (!cb->channel) {
        ERROR("ibv_create_comp_channel failed\n");
        ret = errno;
        goto err1;
    }
    DEBUG("created channel %p\n", cb->channel);

    cb->cq = ibv_create_cq(cm_id->verbs, INFELF_SQ_DEPTH * 2, cb,
                cb->channel, 0);
    if (!cb->cq) {
        ERROR("ibv_create_cq failed\n");
        ret = errno;
        goto err2;
    }
    DEBUG("created cq %p\n", cb->cq);

    ret = ibv_req_notify_cq(cb->cq, 0);
    if (ret) {
        ERROR("ibv_create_cq failed\n");
        ret = errno;
        goto err3;
    }

    ret = infelf_create_qp(cb);
    if (ret) {
        goto err3;
    }
    DEBUG("created qp %p\n", cb->qp);
    return 0;

err3:
    ibv_destroy_cq(cb->cq);
err2:
    ibv_destroy_comp_channel(cb->channel);
err1:
    ibv_dealloc_pd(cb->pd);
    return ret;
}

void *cm_thread(void *arg)
{
    struct infelf_ctrl_t *cb = arg;
    struct rdma_cm_event *event;
    int ret;

    while (1) {
        ret = rdma_get_cm_event(cb->cm_channel, &event);
        if (ret) {
            ERROR("rdma_get_cm_event() failed");
            exit(ret);
        }
        ret = cb->cma_event_handler(event->id, event);
        rdma_ack_cm_event(event);
        if (ret)
            exit(ret);
    }
}

void *cq_thread(void *arg)
{
    struct infelf_ctrl_t *cb = arg;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret;

    DEBUG("cq_thread started.\n");

    while (1) {
        pthread_testcancel();

        ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
        if (ret) {
            ERROR("Failed to get cq event!\n");
            pthread_exit(NULL);
        }
        if (ev_cq != cb->cq) {
            ERROR("Unknown CQ!\n");
            pthread_exit(NULL);
        }
        ret = ibv_req_notify_cq(cb->cq, 0);
        if (ret) {
            ERROR("Failed to set notify!\n");
            pthread_exit(NULL);
        }
        ret = cb->cq_event_handler(cb);
        ibv_ack_cq_events(cb->cq, 1);
        if (ret)
            pthread_exit(NULL);
    }
}

void infelf_format_send(struct infelf_ctrl_t *cb, char *buf, struct ibv_mr *mr)
{
    struct infelf_rdma_info *info = &cb->send_buf;

    info->buf = htobe64((uint64_t) (unsigned long) buf);
    info->rkey = htobe32(mr->rkey);
    info->size = htobe32(cb->buf_size);

    DEBUG("RDMA addr %" PRIx64" rkey %x len %d\n",
          be64toh(info->buf), be32toh(info->rkey), be32toh(info->size));
}

