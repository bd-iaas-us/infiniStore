#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <assert.h>
#include <time.h>
#include "protocol.h"
#include "config.h"
#include <infiniband/verbs.h>
#include <map>

//typedef struct connection connection_t;


 struct Connection{
    //tcp socket
    int sock = 0;

    //rdma connections
    struct ibv_context *ib_ctx = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_qp *qp = NULL;
    int gidx = -1;

    rdma_conn_info_t local_info;
    rdma_conn_info_t remote_info;

    std::map<uintptr_t, struct ibv_mr *> local_mr;
    int rdma_inflight_count = 0;

    Connection()=default;
    Connection (const Connection&) = delete;
    //destory the connection
    ~Connection();
};

typedef struct Connection connection_t;




int init_connection(connection_t *conn, client_config_t config);
//async rw local cpu memory, even rw_local returns, it is not guaranteed that the operation is completed until sync_local is recved.
int rw_local(connection_t *conn, char op, const std::vector<block_t>& blocks, int block_size, void *ptr);
int sync_local(connection_t *conn);
int get_kvmap_len();
int setup_rdma(connection_t *conn, client_config_t config);
int rw_rdma(connection_t *conn, char op, const std::vector<block_t>& blocks, int block_size, void * ptr);

int sync_rdma(connection_t *conn);


#endif // LIBINFINISTORE_H
