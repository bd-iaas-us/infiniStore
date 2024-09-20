#ifndef __INFELF_RDMA_H__
#define __INFELF_RDMA_H__

#include <pthread.h>
#include <inttypes.h>
#include <sys/socket.h>
#include <rdma/rdma_cma.h>
#include "infelf.h"

struct infelf_rdma_info {
    __be64 buf;
    __be32 rkey;
    __be32 size;
};

/*
 * Default max buffer size for IO...
 */
#define INFELF_BUFSIZE 64*1024
#define INFELF_SQ_DEPTH 16

/*
 * Control block struct.
 */
struct infelf_ctrl_t {
    int server;                       /* 0 iff client */
    struct ibv_comp_channel *channel;
    struct ibv_cq *cq;
    struct ibv_pd *pd;
    struct ibv_qp *qp;

    struct ibv_recv_wr rq_wr;         /* recv work request record */
    struct ibv_sge recv_sgl;          /* recv single SGE */
    struct infelf_rdma_info recv_buf; /* malloc'd buffer */
    struct ibv_mr *recv_mr;           /* MR associated with this buffer */

    struct ibv_send_wr sq_wr;         /* send work request record */
    struct ibv_sge send_sgl;
    struct infelf_rdma_info send_buf; /* single send buf */
    struct ibv_mr *send_mr;

    struct ibv_send_wr rdma_sq_wr;    /* rdma work request record */
    struct ibv_sge rdma_sgl;          /* rdma single SGE */
    char *rdma_buf;                   /* used as rdma sink */
    struct ibv_mr *rdma_mr;

    uint32_t remote_rkey;             /* remote guys RKEY */
    uint64_t remote_addr;             /* remote guys TO */
    uint32_t remote_len;              /* remote guys LEN */

    char *start_buf;                  /* rdma read src */
    int buf_size;
    struct ibv_mr *start_mr;

    struct sockaddr_storage sin;
    struct sockaddr_storage ssource;
    __be16 port;                     /* dst port in NBO */

    int (*cq_event_handler)(struct infelf_ctrl_t *cb);
    int (*cma_event_handler)(struct rdma_cm_id *cma_id, struct rdma_cm_event *event);

    /* CM stuff */
    struct rdma_event_channel *cm_channel;
    struct rdma_cm_id *cm_id;        /* connection on client side,*/
                                     /* listener on service side. */
    struct rdma_cm_id *child_cm_id;  /* connection on server side */

    char *gpu_bdf;                   /* allocate buffer on GPU if not NULL*/
    void *app_ctx;                   /* application context */
};

void size_str(char *str, size_t ssize, long long size);
void cnt_str(char *str, size_t ssize, long long cnt);
int size_to_count(int size);
void format_buf(void *buf, int size);
int verify_buf(void *buf, int size);
uint64_t gettime_ns(void);
uint64_t gettime_us(void);
int sleep_us(unsigned int time_us);

struct rdma_event_channel* infelf_create_event_channel(void);
void infelf_init_conn_param(struct rdma_conn_param *conn_param);
int infelf_setup_qp(struct infelf_ctrl_t *cb, struct rdma_cm_id *cm_id);
int infelf_setup_buffers(struct infelf_ctrl_t *cb, int gpu_dev_id, int size);
int infelf_disconnect(struct rdma_cm_id *id);
void infelf_free_buffers(struct infelf_ctrl_t *cb);
void infelf_free_qp(struct infelf_ctrl_t *cb);
void infelf_format_send(struct infelf_ctrl_t *cb, char *buf, struct ibv_mr *mr);
void *cq_thread(void *arg);
void *cm_thread(void *arg);

#endif
