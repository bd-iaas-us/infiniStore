#ifndef LIBINFINITY_H
#define LIBINFINITY_H

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <assert.h>
#include <time.h>
#include "protocol.h"
#include <infiniband/verbs.h>
#include <map>

//typedef struct connection connection_t;


 struct Connection{
    //tcp socket
    int sock;

    //rdma connections
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    int gidx; //gid index

    rdma_conn_info_t local_info;
    rdma_conn_info_t remote_info;
    
    std::map<uintptr_t, struct ibv_mr *> local_mr;

    Connection (const Connection&) = delete;
    //destory the connection
    ~Connection();
};

typedef struct Connection connection_t;




int init_connection(connection_t *conn, std::string ip_addr);
//async rw local cpu memory, even rw_local returns, it is not guaranteed that the operation is completed until sync_local is recved.
int rw_local(connection_t *conn, char op, const std::vector<block_t>& blocks, int block_size, void *ptr);
int sync_local(connection_t *conn);
int get_kvmap_len();
int setup_rdma(connection_t *conn);
int rw_remote(connection_t *conn, char op, const std::vector<std::string>keys, int block_size, void * ptr);

#endif // LIBINFINITY_H