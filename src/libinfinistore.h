#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <arpa/inet.h>
#include <assert.h>
#include <infiniband/verbs.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <future>
#include <map>

#include "config.h"
#include "log.h"
#include "protocol.h"

// typedef struct connection connection_t;

class IBVMemoryRegion {
   public:
    IBVMemoryRegion(struct ibv_pd *pd, void *addr, size_t length) : ref_count_(0) {
        mr_ = ibv_reg_mr(pd, addr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        if (!mr_) {
            throw std::runtime_error("Failed to register memory region");
        }
    }

    ~IBVMemoryRegion() {
        if (mr_) {
            DEBUG("deregister mr: {}", (void *)this);
            ibv_dereg_mr(mr_);
        }
    }

    void add_ref() { ref_count_.fetch_add(1, std::memory_order_relaxed); }

    size_t release() {
        if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            size_t ret = mr_->length;
            delete this;
            return ret;
        }
        return 0;
    }

    struct ibv_mr *get_mr() const {
        return mr_;
    }

   private:
    struct ibv_mr *mr_;
    std::atomic<int> ref_count_;
};

struct Connection {
    // tcp socket
    int sock = 0;

    // rdma connections
    struct ibv_context *ib_ctx = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_qp *qp = NULL;
    int gidx = -1;

    rdma_conn_info_t local_info;
    rdma_conn_info_t remote_info;

    std::map<uintptr_t, IBVMemoryRegion *> local_mr;

    struct ibv_comp_channel *comp_channel = NULL;
    std::future<void> cq_future;  // cq thread
    std::atomic<int> rdma_inflight_count{0};
    std::atomic<bool> stop{false};
    std::mutex mutex;
    std::condition_variable cv;

    // if GPU's bar1 is less than total avaliable memory, we need to set this
    // flag. so every RDMA read/write will have to check if the memory region is
    // bigger than bar1. use have to split the requests
    bool limited_bar1 = false;
    unsigned int bar1_mem_in_mib = 0;
    std::atomic<size_t> rdma_inflight_mr_size{0};

    Connection() = default;
    Connection(const Connection &) = delete;
    // destory the connection
    ~Connection();
};

typedef struct Connection connection_t;

int init_connection(connection_t *conn, client_config_t config);
// async rw local cpu memory, even rw_local returns, it is not guaranteed that
// the operation is completed until sync_local is recved.
int rw_local(connection_t *conn, char op, const std::vector<block_t> &blocks, int block_size,
             void *ptr);
int sync_local(connection_t *conn);
int get_kvmap_len();
int setup_rdma(connection_t *conn, client_config_t config);
int rw_rdma(connection_t *conn, char op, std::vector<block_t> &blocks, int block_size, void *ptr,
            size_t ptr_region_size);

int sync_rdma(connection_t *conn);
int check_exist(connection_t *conn, std::string key);

#endif  // LIBINFINISTORE_H
