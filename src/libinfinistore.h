#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <arpa/inet.h>
#include <assert.h>
#include <infiniband/verbs.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <boost/lockfree/spsc_queue.hpp>
#include <deque>
#include <future>
#include <map>
#include <stdexcept>

#include "config.h"
#include "log.h"
#include "protocol.h"

// RDMA send buffer
// because write_cache will be invoked asynchronously,
// so each request will have a standalone send buffer.
struct SendBuffer {
    void *buffer_ = NULL;
    struct ibv_mr *mr_ = NULL;

    SendBuffer(struct ibv_pd *pd, size_t size);
    SendBuffer(const SendBuffer &) = delete;
    ~SendBuffer();
};

class Connection {
    // tcp socket
    int sock_ = 0;

    // rdma connections
    struct ibv_context *ib_ctx_ = NULL;
    struct ibv_pd *pd_ = NULL;
    struct ibv_cq *cq_ = NULL;
    struct ibv_qp *qp_ = NULL;
    int gidx_ = -1;
    int lid_ = -1;
    uint8_t ib_port_ = -1;

    // local active_mtu attr, after exchanging with remote, we will use the min of the two for
    // path.mtu
    ibv_mtu active_mtu_;

    rdma_conn_info_t local_info_;
    rdma_conn_info_t remote_info_;

    std::unordered_map<uintptr_t, struct ibv_mr *> local_mr_;

    /*
    This is MAX_RECV_WR not MAX_SEND_WR,
    because server also has the same number of buffers
    */
    boost::lockfree::spsc_queue<SendBuffer *> send_buffers_{MAX_RECV_WR};

    // this recv buffer is used in
    // 1. allocate rdma
    // 2. recv IMM data, althougth IMM DATA is not put into recv_buffer,
    // but for compatibility, we still use a zero-length recv_buffer.
    void *recv_buffer_ = NULL;
    struct ibv_mr *recv_mr_ = NULL;

    struct ibv_comp_channel *comp_channel_ = NULL;
    std::future<void> cq_future_;  // cq thread
    std::atomic<int> rdma_inflight_count_{0};

    std::atomic<bool> stop_{false};
    // protect rdma_inflight_count
    std::mutex mutex_;
    std::condition_variable cv_;

    // protect ibv_post_send, outstanding_rdma_writes_queue
    std::mutex rdma_post_send_mutex_;
    std::atomic<int> outstanding_rdma_writes_{0};
    std::deque<std::pair<struct ibv_send_wr *, struct ibv_sge *>> outstanding_rdma_writes_queue_;

   public:
    Connection() = default;

    Connection(const Connection &) = delete;
    // destory the connection
    ~Connection();
    // close cq_handler thread
    void close_conn();
    int init_connection(client_config_t config);
    // async rw local cpu memory, even rw_local returns, it is not guaranteed that
    // the operation is completed until sync_local is recved.
    int rw_local(char op, const std::vector<block_t> &blocks, int block_size, void *ptr,
                 int device_id);
    int sync_local();
    int setup_rdma(client_config_t config);
    int r_rdma(std::vector<block_t> &blocks, int block_size, void *base_ptr);
    int r_rdma_async(std::vector<block_t> &blocks, int block_size, void *base_ptr,
                     std::function<void()> callback);
    int w_rdma(unsigned long *p_offsets, size_t offsets_len, int block_size,
               remote_block_t *p_remote_blocks, size_t remote_blocks_len, void *base_ptr);
    int w_rdma_async(unsigned long *p_offsets, size_t offsets_len, int block_size,
                     remote_block_t *p_remote_blocks, size_t remote_blocks_len, void *base_ptr,
                     std::function<void()> callback);
    int sync_rdma();
    std::vector<remote_block_t> *allocate_rdma(std::vector<std::string> &keys, int block_size);
    int allocate_rdma_async(std::vector<std::string> &keys, int block_size,
                            std::function<void(std::vector<remote_block_t> *)> callback);
    int check_exist(std::string key);
    int get_match_last_index(std::vector<std::string>);
    int register_mr(void *base_ptr, size_t ptr_region_size);

    int modify_qp_to_init();
    int modify_qp_to_rts();
    int modify_qp_to_rtr();
    int exchange_conn_info();
    int init_rdma_resources(client_config_t config);

    void cq_handler();
    // TODO: refactor to c++ style
    SendBuffer *get_send_buffer();
    void release_send_buffer(SendBuffer *buffer);
};

struct rdma_read_commit_info {
    // call back function.
    std::function<void()> callback;
    rdma_read_commit_info(std::function<void()> callback) : callback(callback) {}
};

struct rdma_write_commit_info {
    // call back function.
    std::function<void()> callback;
    // the number of blocks that have been written.
    std::vector<uintptr_t> remote_addrs;

    rdma_write_commit_info(std::function<void()> callback, int n)
        : callback(callback), remote_addrs() {
        remote_addrs.reserve(n);
    }
};

#endif  // LIBINFINISTORE_H
