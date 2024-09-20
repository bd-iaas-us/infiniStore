#ifndef __INFINIELF_EXAMPLE_H__
#define __INFINIELF_EXAMPLE_H__

#define INFELF_TEST_MSG_FMT           "infelf_test: "
#define INFELF_TEST_MIN_BUFSIZE       sizeof(stringify(INT_MAX)) + sizeof(INFELF_TEST_MSG_FMT)

#include <pthread.h>

/*
 * These states are used to signal events between the completion handler
 * and the main client or server thread.
 *
 * Once CONNECTED, they cycle through RDMA_READ_ADV, RDMA_WRITE_ADV,
 * and RDMA_WRITE_COMPLETE for each ping.
 */
enum test_state {
    IDLE = 1,
    CONNECT_REQUEST,
    ADDR_RESOLVED,
    ROUTE_RESOLVED,
    CONNECTED,
    RDMA_READ_ADV,
    RDMA_READ_COMPLETE,
    RDMA_WRITE_ADV,
    RDMA_WRITE_COMPLETE,
    DISCONNECTED,
    ERROR
};

/*
 * Example payload struct.
 */
struct app_ctx_t {
    pthread_t cqthread;
    pthread_t cmthread;

    enum test_state state;          /* used for cond/signalling */
    int verbose;                    /* verbose logging */
    int size;                       /* ping data size */
};

int run_server(struct infelf_ctrl_t *listening_cb);
int run_client(struct infelf_ctrl_t *cb);
int server_recv(struct infelf_ctrl_t *cb, struct ibv_wc *wc);
int client_recv(struct infelf_ctrl_t *cb, struct ibv_wc *wc);

#endif
