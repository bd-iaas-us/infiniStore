#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <arpa/inet.h>
#include <assert.h>
#include <infiniband/verbs.h>
#include <time.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>
#include <deque>
#include <future>
#include <map>

#include "config.h"
#include "log.h"
#include "protocol.h"

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

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

struct Connection {
    // tcp socket

    asio::io_context io_context;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard;
    tcp::socket socket;
    tcp::resolver resolver;

    // rdma connections
    struct ibv_context *ib_ctx = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_qp *qp = NULL;
    int gidx = -1;
    int lid = -1;
    uint8_t ib_port = -1;

    // local active_mtu attr, after exchanging with remote, we will use the min of the two for
    // path.mtu
    ibv_mtu active_mtu;

    rdma_conn_info_t local_info;
    rdma_conn_info_t remote_info;

    std::unordered_map<uintptr_t, struct ibv_mr *> local_mr;

    /*
    This is MAX_RECV_WR not MAX_SEND_WR,
    because server also has the same number of buffers
    */
    boost::lockfree::queue<SendBuffer *> send_buffers{MAX_RECV_WR};

    // this recv buffer is used in
    // 1. allocate rdma
    // 2. recv IMM data, althougth IMM DATA is not put into recv_buffer,
    // but for compatibility, we still use a zero-length recv_buffer.
    void *recv_buffer = NULL;
    struct ibv_mr *recv_mr = NULL;

    struct ibv_comp_channel *comp_channel = NULL;
    std::thread cq_thread;  // cq thread
    std::atomic<int> rdma_inflight_count{0};

    std::atomic<bool> stop{false};
    // protect rdma_inflight_count
    std::mutex mutex;
    std::condition_variable cv;

    // protect ibv_post_send, outstanding_rdma_writes_queue
    std::mutex rdma_post_send_mutex;
    std::atomic<int> outstanding_rdma_writes{0};
    std::deque<std::pair<struct ibv_send_wr *, struct ibv_sge *>> outstanding_rdma_writes_queue;

    std::thread asio_thread;
    Connection()
        : io_context(),
          work_guard(boost::asio::make_work_guard(io_context)),
          socket(io_context),
          resolver(io_context) {
        asio_thread = std::thread([this]() { io_context.run(); });
    }

    Connection(const Connection &) = delete;
    // destory the connection
    ~Connection();
    void _close();
};

typedef struct Connection connection_t;

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

int init_connection(connection_t *conn, client_config_t config);
void init_connection_async(connection_t *conn, client_config_t config,
                           std::function<void(int)> callback);
// async rw local cpu memory, even rw_local returns, it is not guaranteed that
// the operation is completed until sync_local is recved.
int rw_local(connection_t *conn, char op, const std::vector<block_t> &blocks, int block_size,
             void *ptr, int device_id);
int sync_local(connection_t *conn);
int setup_rdma(connection_t *conn, client_config_t config);
int setup_rdma_async(connection_t *conn, client_config_t config, std::function<void(int)> callback);
int r_rdma(connection_t *conn, std::vector<block_t> &blocks, int block_size, void *base_ptr);
int r_rdma_async(connection_t *conn, std::vector<block_t> &blocks, int block_size, void *base_ptr,
                 std::function<void()> callback);
int w_rdma(connection_t *conn, unsigned long *p_offsets, size_t offsets_len, int block_size,
           remote_block_t *p_remote_blocks, size_t remote_blocks_len, void *base_ptr);
int w_rdma_async(connection_t *conn, unsigned long *p_offsets, size_t offsets_len, int block_size,
                 remote_block_t *p_remote_blocks, size_t remote_blocks_len, void *base_ptr,
                 std::function<void()> callback);
int sync_rdma(connection_t *conn);
std::vector<remote_block_t> *allocate_rdma(connection_t *conn, std::vector<std::string> &keys,
                                           int block_size);
int allocate_rdma_async(connection_t *conn, std::vector<std::string> &keys, int block_size,
                        std::function<void(std::vector<remote_block_t> *)> callback);
int check_exist(connection_t *conn, std::string key);
int get_match_last_index(connection_t *conn, std::vector<std::string>);
int register_mr(connection_t *conn, void *base_ptr, size_t ptr_region_size);

// TODO: refactor to c++ style
SendBuffer *get_send_buffer(connection_t *conn);
void release_send_buffer(connection_t *conn, SendBuffer *buffer);

#endif  // LIBINFINISTORE_H
