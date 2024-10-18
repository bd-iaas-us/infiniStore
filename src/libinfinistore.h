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
#include <future>

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


    struct ibv_comp_channel *comp_channel = NULL;
    std::future<void> cq_future; //cq thread
    std::atomic<int> rdma_inflight_count{0};
    std::atomic<bool> stop{false};
    std::mutex mutex;
    std::condition_variable cv;


    //if GPU's bar1 is less than total avaliable memory, we need to set this flag.
    //so every RDMA read/write will have to check if the memory region is bigger than bar1. use have to split the requests
    //
    bool limited_bar1 = false;
    unsigned int bar1_mem_in_mib = 0;


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
int rw_rdma(connection_t *conn, char op, std::vector<block_t>& blocks, int block_size, void * ptr, size_t ptr_region_size);


int sync_rdma(connection_t *conn);


#endif // LIBINFINISTORE_H
