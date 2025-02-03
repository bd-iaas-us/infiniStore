// single thread right now.
#include "infinistore.h"

#include <arpa/inet.h>
#include <assert.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <boost/lockfree/spsc_queue.hpp>
#include <chrono>
#include <deque>
#include <future>
#include <iostream>
#include <string>
#include <unordered_map>

#include "ibv_helper.h"
#include "protocol.h"

server_config_t global_config;

uv_loop_t *loop;
uv_tcp_t server;
// global ibv context
struct ibv_context *ib_ctx;
struct ibv_pd *pd;
MM *mm;

int gidx = 0;
int lid = -1;
uint8_t ib_port = -1;
// local active_mtu attr, after exchanging with remote, we will use the min of the two for path.mtu
ibv_mtu active_mtu;

// indicate if the MM extend is in flight
bool extend_in_flight = false;
// indicate the number of cudaIpcOpenMemHandle
std::atomic<unsigned int> opened_ipc{0};

enum CUDA_TASK_TYPE {
    CUDA_READ,
    CUDA_WRITE,
    QUIT,
};

struct CUDA_TASK {
    std::vector<boost::intrusive_ptr<PTR>> ptrs;
    enum CUDA_TASK_TYPE type;
    int device = -1;
    void *d_ptr = NULL;
    cudaStream_t stream;
    cudaEvent_t event;
};

std::unordered_map<uintptr_t, boost::intrusive_ptr<PTR>> inflight_rdma_writes;

std::unordered_map<std::string, boost::intrusive_ptr<PTR>> kv_map;

typedef enum {
    READ_HEADER,
    READ_BODY,
} read_state_t;

struct Client {
    uv_tcp_t *handle_ = NULL;    // uv_stream_t
    read_state_t state_;         // state of the client, for parsing the request
    size_t bytes_read_ = 0;      // bytes read so far, for parsing the request
    size_t expected_bytes_ = 0;  // expected size of the body
    header_t header_;

    // RDMA recv buffer
    char *recv_buffer_[MAX_RECV_WR] = {};
    struct ibv_mr *recv_mr_[MAX_RECV_WR] = {};

    // RDMA send buffer
    char *send_buffer_ = NULL;
    struct ibv_mr *send_mr_ = NULL;
    int outstanding_rdma_writes_ = 0;
    std::deque<std::pair<struct ibv_send_wr *, struct ibv_sge *>> outstanding_rdma_writes_queue_;

    // TCP send buffer
    char *tcp_send_buffer_ = NULL;
    char *tcp_recv_buffer_ = NULL;

    rdma_conn_info_t remote_info_;
    rdma_conn_info_t local_info_;

    struct ibv_cq *cq_ = NULL;
    struct ibv_qp *qp_ = NULL;
    bool rdma_connected_ = false;
    struct ibv_comp_channel *comp_channel_ = NULL;

    std::atomic<unsigned int> remain_{0};
    // notify thread new request
    uv_sem_t sem_;
    // we use this thread to wait for the completion of the local GPU operation
    std::future<void> cuda_future_;

    using CudaTaskQueue = boost::lockfree::spsc_queue<CUDA_TASK *, boost::lockfree::capacity<4096>>;
    std::shared_ptr<CudaTaskQueue> cuda_task_queue_ = std::make_shared<CudaTaskQueue>();

    uv_poll_t poll_handle_;

    Client() = default;
    Client(const Client &) = delete;
    ~Client();

    void cq_poll_handle(uv_poll_t *handle, int status, int events);
    int read_rdma_cache(const RemoteMetaRequest *req);
    int allocate_rdma(const RemoteMetaRequest *req);
    int read_cache(const LocalMetaRequest *meta_req);
    int write_cache(const LocalMetaRequest *meta_req);
    // send response to client through TCP
    void send_resp(int return_code, void *buf, size_t size);
    int sync_stream();
    void reset_client_read_state();
    int check_key(const std::string &key_to_check);
    int get_match_last_index(const GetMatchLastIndexRequest *request);
    int rdma_exchange();
    int prepare_recv_rdma_request(int buf_idx);
    void wait_for_ipc_close(std::shared_ptr<CudaTaskQueue> cuda_task_queue);
};

typedef struct Client client_t;

Client::~Client() {
    INFO("free client resources");

    if (cuda_future_.valid()) {
        // send quit signal to the waiting_for_ipc_close thread
        CUDA_TASK *task = new CUDA_TASK();
        task->type = QUIT;
        // cuda_task_queue_.push(task);
        cuda_task_queue_->push(task);
        uv_sem_post(&sem_);
        // cuda_future_.get().get() could block the main thread. we do not wait for it.
    }

    if (poll_handle_.data) {
        uv_poll_stop(&poll_handle_);
    }

    if (handle_) {
        free(handle_);
        handle_ = NULL;
    }

    if (send_mr_) {
        ibv_dereg_mr(send_mr_);
        send_mr_ = NULL;
    }

    if (send_buffer_) {
        free(send_buffer_);
        send_buffer_ = NULL;
    }

    for (int i = 0; i < MAX_RECV_WR; i++) {
        if (recv_mr_[i]) {
            assert(recv_buffer_[i] != NULL);
            ibv_dereg_mr(recv_mr_[i]);
            recv_mr_[i] = NULL;
        }

        if (recv_buffer_[i]) {
            free(recv_buffer_[i]);
            recv_buffer_[i] = NULL;
        }
    }

    if (tcp_send_buffer_) {
        free(tcp_send_buffer_);
        tcp_send_buffer_ = NULL;
    }

    if (tcp_recv_buffer_) {
        free(tcp_recv_buffer_);
        tcp_recv_buffer_ = NULL;
    }

    if (qp_) {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE)) {
            ERROR("Failed to modify QP to ERR state");
        }
    }
    if (qp_) {
        ibv_destroy_qp(qp_);
        qp_ = NULL;
    }
    if (cq_) {
        ibv_destroy_cq(cq_);
        cq_ = NULL;
    }

    if (comp_channel_) {
        ibv_destroy_comp_channel(comp_channel_);
        comp_channel_ = NULL;
    }
}

void Client::cq_poll_handle(uv_poll_t *handle, int status, int events) {
    DEBUG("Polling CQ");

    // TODO: handle completion
    if (status < 0) {
        ERROR("Poll error: {}", uv_strerror(status));
        return;
    }
    struct ibv_cq *cq;
    void *cq_context;

    if (ibv_get_cq_event(comp_channel_, &cq, &cq_context) != 0) {
        ERROR("Failed to get CQ event");
        return;
    }
    ibv_ack_cq_events(cq, 1);

    if (ibv_req_notify_cq(cq, 0) != 0) {
        ERROR("Failed to request CQ notification");
        return;
    }
    struct ibv_wc wc = {0};
    while (ibv_poll_cq(cq, 1, &wc) > 0) {
        if (wc.status == IBV_WC_SUCCESS) {
            if (wc.opcode == IBV_WC_RECV) {  // recv RDMA read/write request
                INFO("RDMA Send completed successfully, recved {}", wc.byte_len);
                const RemoteMetaRequest *request = GetRemoteMetaRequest(recv_buffer_[wc.wr_id]);

                INFO("Received remote meta request OP {}", op_name(request->op()));

                switch (request->op()) {
                    case OP_RDMA_READ:
                        read_rdma_cache(request);
                        break;
                    case OP_RDMA_ALLOCATE: {
                        auto start = std::chrono::high_resolution_clock::now();
                        allocate_rdma(request);
                        INFO("allocate_rdma time: {} micro seconds",
                             std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::high_resolution_clock::now() - start)
                                 .count());
                        break;
                    }
                    case OP_RDMA_WRITE_COMMIT: {
                        INFO("RDMA write commit, #addrs: {}", request->remote_addrs()->size());
                        if (request->remote_addrs()->size() == 0) {
                            ERROR("remote_addrs size should not be 0");
                        }
                        for (auto addr : *request->remote_addrs()) {
                            auto it = inflight_rdma_writes.find(addr);
                            if (it == inflight_rdma_writes.end()) {
                                ERROR("commit msg: Key not found: {}", addr);
                                continue;
                            }
                            it->second->committed = true;
                            inflight_rdma_writes.erase(it);
                        }
                        DEBUG("inflight_rdma_kv_map size: {}", inflight_rdma_writes.size());
                        break;
                    }
                    default:
                        ERROR("Unexpected request op: {}", request->op());
                        break;
                }

                INFO("ready for next request");
                if (prepare_recv_rdma_request(wc.wr_id) < 0) {
                    ERROR("Failed to prepare recv rdma request");
                    return;
                }
            }
            else if (wc.opcode == IBV_WC_SEND) {  // allocate: response sent
                DEBUG("allocate response sent");
            }
            else if (wc.opcode ==
                     IBV_WC_RECV_RDMA_WITH_IMM) {  // write cache: we alreay have all data now.
                // client should not use WRITE_WITH_IMM to notify.
                // it should use COMMIT message to notify.
                WARN("WRITE_WITH_IMM is not supported in server side");
                if (prepare_recv_rdma_request(wc.wr_id) < 0) {
                    ERROR("Failed to prepare recv rdma request");
                    return;
                }
            }
            else if (wc.opcode == IBV_WC_RDMA_WRITE) {
                // some RDMA write(read cache WRs) is finished

                DEBUG("RDMA_WRITE done wr_id: {}", wc.wr_id);
                assert(outstanding_rdma_writes_ >= 0);
                outstanding_rdma_writes_ -= MAX_WR_BATCH;

                if (!outstanding_rdma_writes_queue_.empty()) {
                    auto item = outstanding_rdma_writes_queue_.front();
                    struct ibv_send_wr *wrs = item.first;
                    struct ibv_sge *sges = item.second;
                    ibv_send_wr *bad_wr = nullptr;
                    DEBUG("IBV POST SEND, wr_id: {}", wrs[0].wr_id);
                    int ret = ibv_post_send(qp_, &wrs[0], &bad_wr);
                    if (ret) {
                        ERROR("Failed to post RDMA write {}", strerror(ret));
                        throw std::runtime_error("Failed to post RDMA write");
                    }
                    outstanding_rdma_writes_ += MAX_WR_BATCH;
                    delete[] wrs;
                    delete[] sges;
                    outstanding_rdma_writes_queue_.pop_front();
                }

                if (wc.wr_id > 0) {
                    // last WR will inform that all RDMA write is finished,so we can dereference PTR
                    auto inflight_rdma_reads = (std::vector<boost::intrusive_ptr<PTR>> *)wc.wr_id;
                    delete inflight_rdma_reads;
                }
            }
            else {
                ERROR("Unexpected wc opcode: {}", (int)wc.opcode);
            }
        }
        else {
            ERROR("CQ error: {}, {}", ibv_wc_status_str(wc.status), wc.wr_id);
        }
    }
}

int Client::allocate_rdma(const RemoteMetaRequest *req) {
    INFO("do allocate_rdma...");

    FixedBufferAllocator allocator(send_buffer_, PROTOCOL_BUFFER_SIZE);
    FlatBufferBuilder builder(64 << 10, &allocator);

    int key_idx = 0;
    int block_size = req->block_size();
    std::vector<RemoteBlock> blocks;
    blocks.reserve(req->keys()->size());

    if (!mm->allocate(block_size, req->keys()->size(),
                      [&](void *addr, uint32_t lkey, uint32_t rkey, int pool_idx) {
                          // FIXME: rdma write should have a msg to update committed to true

                          const auto *key = req->keys()->Get(key_idx);

                          if (kv_map.count(key->str()) != 0) {
                              // WARN("rdma_write: Key already exists: {}", key->str());
                              // put fake addr, and send to client
                              blocks.push_back(FAKE_REMOTE_BLOCK);
                              key_idx++;
                              return;
                          }

                          auto ptr =
                              boost::intrusive_ptr<PTR>(new PTR(addr, block_size, pool_idx, false));

                          // save in kv_map, but committed is false, no one can read it
                          kv_map[key->str()] = ptr;

                          // save in inflight_rdma_kv_map, when write is finished, we can merge it
                          // into kv_map
                          inflight_rdma_writes[(uintptr_t)addr] = ptr;

                          blocks.push_back(RemoteBlock(rkey, (uint64_t)addr));
                          key_idx++;
                      })) {
        ERROR("Failed to allocate memory");
        return SYSTEM_ERROR;
    }

    auto resp = CreateRdmaAllocateResponseDirect(builder, &blocks);
    builder.Finish(resp);

    // send RDMA request
    struct ibv_sge sge = {0};
    struct ibv_send_wr wr = {0};
    struct ibv_send_wr *bad_wr = NULL;

    sge.addr = (uintptr_t)builder.GetBufferPointer();
    sge.length = builder.GetSize();
    sge.lkey = send_mr_->lkey;

    wr.wr_id = 0;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    int ret = ibv_post_send(qp_, &wr, &bad_wr);
    if (ret) {
        ERROR("Failed to post RDMA send :{}", strerror(ret));
        return -1;
    }

    return 0;
}

int Client::prepare_recv_rdma_request(int buf_idx) {
    struct ibv_sge sge = {0};
    struct ibv_recv_wr rwr = {0};
    struct ibv_recv_wr *bad_wr = NULL;
    sge.addr = (uintptr_t)(recv_buffer_[buf_idx]);
    sge.length = PROTOCOL_BUFFER_SIZE;
    sge.lkey = recv_mr_[buf_idx]->lkey;

    rwr.wr_id = buf_idx;
    rwr.next = NULL;
    rwr.sg_list = &sge;
    rwr.num_sge = 1;
    if (ibv_post_recv(qp_, &rwr, &bad_wr)) {
        ERROR("Failed to post receive, {}");
        return -1;
    }
    return 0;
}

int Client::read_rdma_cache(const RemoteMetaRequest *remote_meta_req) {
    INFO("do rdma read... num of keys: {}", remote_meta_req->keys()->size());

    if (remote_meta_req->keys()->size() != remote_meta_req->remote_addrs()->size()) {
        ERROR("keys size and remote_addrs size mismatch");
        return -1;
    }

    auto *inflight_rdma_reads = new std::vector<boost::intrusive_ptr<PTR>>;

    inflight_rdma_reads->reserve(remote_meta_req->keys()->size());

    for (const auto *key : *remote_meta_req->keys()) {
        auto it = kv_map.find(key->str());
        if (it == kv_map.end()) {
            ERROR("Key not found: {}", key->str());
            return -1;
        }

        if (!it->second->committed) {
            ERROR("Key not committed: {}", key->str());
            return -1;
        }

        const auto &ptr = it->second;

        DEBUG("rkey: {}, local_addr: {}, size : {}", mm->get_lkey(ptr->pool_idx),
              (uintptr_t)ptr->ptr, ptr->size);

        inflight_rdma_reads->push_back(ptr);
    }

    const size_t max_wr = MAX_WR_BATCH;
    struct ibv_send_wr local_wrs[max_wr];
    struct ibv_sge local_sges[max_wr];

    struct ibv_send_wr *wrs = local_wrs;
    struct ibv_sge *sges = local_sges;

    size_t num_wr = 0;
    bool wr_full = false;

    if (outstanding_rdma_writes_ + max_wr > MAX_RDMA_WRITE_WR) {
        wr_full = true;
        wrs = new struct ibv_send_wr[max_wr];
        sges = new struct ibv_sge[max_wr];
    }
    for (size_t i = 0; i < remote_meta_req->keys()->size(); i++) {
        sges[num_wr].addr = (uintptr_t)(*inflight_rdma_reads)[i]->ptr;
        sges[num_wr].length = remote_meta_req->block_size();
        sges[num_wr].lkey = mm->get_lkey((*inflight_rdma_reads)[i]->pool_idx);

        wrs[num_wr].wr_id = 0;
        wrs[num_wr].opcode = (i == remote_meta_req->keys()->size() - 1) ? IBV_WR_RDMA_WRITE_WITH_IMM
                                                                        : IBV_WR_RDMA_WRITE;
        wrs[num_wr].sg_list = &sges[num_wr];
        wrs[num_wr].num_sge = 1;
        wrs[num_wr].wr.rdma.remote_addr = remote_meta_req->remote_addrs()->Get(i);
        wrs[num_wr].wr.rdma.rkey = remote_meta_req->rkey();
        wrs[num_wr].next = (num_wr == max_wr - 1 || i == remote_meta_req->keys()->size() - 1)
                               ? nullptr
                               : &wrs[num_wr + 1];

        wrs[num_wr].send_flags = (num_wr == max_wr - 1 || i == remote_meta_req->keys()->size() - 1)
                                     ? IBV_SEND_SIGNALED
                                     : 0;

        if (i == remote_meta_req->keys()->size() - 1) {
            wrs[num_wr].wr_id = (uintptr_t)inflight_rdma_reads;
        }

        num_wr++;

        if (num_wr == max_wr || i == remote_meta_req->keys()->size() - 1) {
            if (!wr_full) {
                struct ibv_send_wr *bad_wr = nullptr;
                DEBUG("local write");
                int ret = ibv_post_send(qp_, &wrs[0], &bad_wr);
                if (ret) {
                    ERROR("Failed to post RDMA write {}", strerror(ret));
                    return -1;
                }
                outstanding_rdma_writes_ += max_wr;

                // check if next iteration will exceed the limit
                if (outstanding_rdma_writes_ + max_wr > MAX_RDMA_WRITE_WR) {
                    wr_full = true;
                }
            }
            else {
                // if WR queue is full, we need to put them into queue
                WARN(
                    "WR queue full: push into queue, len: {}, first wr_id: {}, last wr_id: {}, "
                    "last op code: {} ",
                    num_wr, wrs[0].wr_id, wrs[num_wr - 1].wr_id,
                    static_cast<int>(wrs[num_wr - 1].opcode));
                outstanding_rdma_writes_queue_.push_back({&wrs[0], &sges[0]});
            }

            if (wr_full) {
                wrs = new struct ibv_send_wr[max_wr];
                sges = new struct ibv_sge[max_wr];
            }

            num_wr = 0;  // Reset the counter for the next batch
        }
    }

    return 0;
}

// FIXME:
void Client::reset_client_read_state() {
    state_ = READ_HEADER;
    bytes_read_ = 0;
    expected_bytes_ = FIXED_HEADER_SIZE;
    memset(&header_, 0, sizeof(header_t));
    // keep the tcp_recv_buffer/tcp_send_buffer as it is
}

void on_close(uv_handle_t *handle) {
    client_t *client = (client_t *)handle->data;
    delete client;
}

void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = (char *)malloc(suggested_size);
    buf->len = suggested_size;
}

int veryfy_header(header_t *header) {
    if (header->magic != MAGIC) {
        return INVALID_REQ;
    }
    // TODO: add more checks
    return 0;
}

void on_write(uv_write_t *req, int status) {
    if (status < 0) {
        ERROR("Write error {}", uv_strerror(status));
        uv_close((uv_handle_t *)req->handle, on_close);
    }
    free(req);
}

int Client::read_cache(const LocalMetaRequest *meta_req) {
    INFO("do read_cache...");

    const header_t *header = &this->header_;
    void *d_ptr;

    assert(header != NULL);
    // TODO: check device_id

    CHECK_CUDA(cudaSetDevice(meta_req->device()));

    cudaEvent_t event;
    // create events
    CHECK_CUDA(cudaEventCreate(&event));

    cudaStream_t cuda_stream;
    CHECK_CUDA(cudaStreamCreate(&cuda_stream));

    cudaIpcMemHandle_t ipc_handle = *(cudaIpcMemHandle_t *)meta_req->ipc_handle()->data();
    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    opened_ipc++;

    size_t block_size = meta_req->block_size();
    int idx = 0;

    CUDA_TASK *task = new CUDA_TASK();
    task->ptrs.reserve(meta_req->blocks()->size());
    std::vector<uintptr_t> remote_addrs;

    for (auto *block : *meta_req->blocks()) {
        auto key = block->key()->str();
        if (kv_map.count(key) == 0 || kv_map[key]->committed == false) {
            ERROR("Key not found: {}", key);
            CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
            CHECK_CUDA(cudaEventDestroy(event));
            CHECK_CUDA(cudaStreamDestroy(cuda_stream));
            delete task;
            return KEY_NOT_FOUND;
        }

        void *h_src = kv_map[key]->ptr;
        assert(h_src != NULL);

        DEBUG("key: {}, local_addr: {}, size : {}", key, (uintptr_t)h_src, block_size);

        task->ptrs.push_back(kv_map[key]);  // keep PTR in task as reference count.
        remote_addrs.push_back((uintptr_t)((char *)d_ptr + block->offset()));
        idx++;
    }

    assert(task->ptrs.size() == remote_addrs.size());

    for (int i = 0; i < task->ptrs.size(); i++) {
        CHECK_CUDA(cudaMemcpyAsync((void *)remote_addrs[i], task->ptrs[i]->ptr, block_size,
                                   cudaMemcpyHostToDevice, cuda_stream));
    }

    CHECK_CUDA(cudaEventRecord(event, cuda_stream));

    task->device = meta_req->device();
    task->stream = cuda_stream;
    task->event = event;
    task->type = CUDA_READ;
    task->d_ptr = d_ptr;

    remain_++;

    if (!cuda_future_.valid()) {
        // initiate the semaphore
        uv_sem_init(&sem_, 0);
        cuda_future_ =
            std::async(std::launch::async, [&]() { this->wait_for_ipc_close(cuda_task_queue_); });
    }

    cuda_task_queue_->push(task);
    uv_sem_post(&sem_);  // wake up thread to clean up

    send_resp(TASK_ACCEPTED, NULL, 0);

    reset_client_read_state();

    return 0;
}

void Client::wait_for_ipc_close(std::shared_ptr<CudaTaskQueue> cuda_task_queue) {
    CUDA_TASK *task = NULL;
    while (true) {
        uv_sem_wait(&sem_);
        cuda_task_queue->pop(task);
        assert(task != NULL);
        assert(task->type == CUDA_WRITE || task->type == CUDA_READ || task->type == QUIT);
        if (task->type == QUIT) {
            // clean up
            delete task;
            break;
        }

        CHECK_CUDA(cudaSetDevice(task->device));
        CHECK_CUDA(cudaEventSynchronize(task->event));
        CHECK_CUDA(cudaEventDestroy(task->event));
        CHECK_CUDA(cudaStreamDestroy(task->stream));
        CHECK_CUDA(cudaIpcCloseMemHandle(task->d_ptr));
        opened_ipc--;
        DEBUG("CUDA_TASK done");

        if (task->type == CUDA_WRITE) {
            // WARN YOU SHOULD NOT access kv_map or inflight_rdma_kv_map in this thread
            for (auto &ptr : task->ptrs) {
                ptr->committed = true;
            }
        }
        remain_--;

        delete task;
    }
    uv_sem_destroy(&sem_);
    INFO("quit the waiting_for_ipc_close thread");
}

void add_mempool(uv_work_t *req) {
    while (opened_ipc > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    mm->add_mempool(pd);
}

void add_mempool_completion(uv_work_t *req, int status) {
    extend_in_flight = false;
    mm->need_extend = false;
    delete req;
}

int Client::write_cache(const LocalMetaRequest *meta_req) {
    INFO("do write_cache..., num of blocks: {}", meta_req->blocks()->size());

    void *d_ptr;
    int return_code = TASK_ACCEPTED;

    cudaIpcMemHandle_t ipc_handle = *(cudaIpcMemHandle_t *)meta_req->ipc_handle()->data();

    CHECK_CUDA(cudaSetDevice(meta_req->device()));

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    opened_ipc++;

    int key_idx = 0;
    size_t block_size = meta_req->block_size();
    size_t num_of_blocks = meta_req->blocks()->size();

    CUDA_TASK *task = new CUDA_TASK();

    auto start = std::chrono::high_resolution_clock::now();

    cudaEvent_t event;

    CHECK_CUDA(cudaEventCreate(&event));

    // create streams per request.
    cudaStream_t cuda_stream;
    CHECK_CUDA(cudaStreamCreate(&cuda_stream));

    // allocate host memory
    bool ret = mm->allocate(
        block_size, num_of_blocks, [&](void *addr, uint32_t lkey, uint32_t rkey, int pool_idx) {
            auto block = meta_req->blocks()->Get(key_idx);
            DEBUG("key: {}, local_addr: {}, size : {}", block->key()->str(), (uintptr_t)addr,
                  block_size);

            // deduplicate the key
            const auto &key = block->key()->str();
            if (kv_map.count(key) != 0) {
                // this key could be commited or uncommitted, no mather what it is, we should skip
                // it
                DEBUG("local gpu write: Key already exists: {}, skip this key", key);
                key_idx++;
                return;
            }
            CHECK_CUDA(cudaMemcpyAsync((void *)addr, (void *)((char *)d_ptr + block->offset()),
                                       block_size, cudaMemcpyDeviceToHost, cuda_stream));

            auto ptr = boost::intrusive_ptr<PTR>(new PTR(addr, block_size, pool_idx, false));
            kv_map[key] = ptr;
            task->ptrs.push_back(ptr);
            key_idx++;
        });
    if (global_config.auto_increase && mm->need_extend && !extend_in_flight) {
        INFO("Extend another mempool");
        uv_work_t *req = new uv_work_t();
        uv_queue_work(loop, req, add_mempool, add_mempool_completion);
        extend_in_flight = true;
    }

    CHECK_CUDA(cudaEventRecord(event, cuda_stream));

    INFO("local gpu write:allocate memory time: {} micro seconds",
         std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::high_resolution_clock::now() - start)
             .count());

    if (!ret) {
        ERROR("Failed to allocate memory");
        delete task;
        return OUT_OF_MEMORY;
    }

    if (task->ptrs.size() == 0) {
        DEBUG("all keys are duplicated, do not start the async tasks");
        delete task;
        send_resp(return_code, NULL, 0);
        reset_client_read_state();
        return 0;
    }

    task->device = meta_req->device();
    task->stream = cuda_stream;
    task->event = event;
    task->type = CUDA_WRITE;
    task->d_ptr = d_ptr;

    remain_++;

    if (!cuda_future_.valid()) {
        // initiate the semaphore
        uv_sem_init(&sem_, 0);
        cuda_future_ =
            std::async(std::launch::async, [&]() { this->wait_for_ipc_close(cuda_task_queue_); });
    }
    cuda_task_queue_->push(task);
    uv_sem_post(&sem_);  // wake up thread to clean up

    send_resp(return_code, NULL, 0);
    reset_client_read_state();

    return 0;
}

int init_rdma_context(server_config_t config) {
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    int num_devices;
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        ERROR("Failed to get RDMA devices list");
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        char *dev_name_from_list = (char *)ibv_get_device_name(dev_list[i]);
        if (strcmp(dev_name_from_list, config.dev_name.c_str()) == 0) {
            INFO("found device {}", dev_name_from_list);
            ib_dev = dev_list[i];
            ib_ctx = ibv_open_device(ib_dev);
            break;
        }
    }

    if (!ib_ctx) {
        INFO(
            "Can't find or failed to open the specified device, try to open "
            "the default device {}",
            (char *)ibv_get_device_name(dev_list[0]));
        ib_ctx = ibv_open_device(dev_list[0]);
        if (!ib_ctx) {
            ERROR("Failed to open the default device");
            return -1;
        }
    }

    struct ibv_port_attr port_attr;
    ib_port = config.ib_port;
    if (ibv_query_port(ib_ctx, ib_port, &port_attr)) {
        ERROR("Unable to query port {} attributes\n", ib_port);
        return -1;
    }
    if ((port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND && config.link_type == "Ethernet") ||
        (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET && config.link_type == "IB")) {
        ERROR("port link layer and config link type don't match");
        return -1;
    }
    if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        gidx = -1;
    }
    else {
        gidx = ibv_find_sgid_type(ib_ctx, ib_port, IBV_GID_TYPE_ROCE_V2, AF_INET);
        if (gidx < 0) {
            ERROR("Failed to find GID");
            return -1;
        }
    }

    lid = port_attr.lid;
    active_mtu = port_attr.active_mtu;

    pd = ibv_alloc_pd(ib_ctx);
    if (!pd) {
        ERROR("Failed to allocate PD");
        return -1;
    }

    return 0;
}

int Client::rdma_exchange() {
    INFO("do rdma exchange...");

    int ret;

    if (rdma_connected_ == true) {
        ERROR("RDMA already connected");
        return SYSTEM_ERROR;
    }

    comp_channel_ = ibv_create_comp_channel(ib_ctx);
    if (!comp_channel_) {
        ERROR("Failed to create completion channel");
        return -1;
    }

    // RDMA setup if not already done
    assert(comp_channel_ != NULL);

    cq_ = ibv_create_cq(ib_ctx, MAX_SEND_WR + MAX_RECV_WR, NULL, comp_channel_, 0);
    if (!cq_) {
        ERROR("Failed to create CQ");
        return SYSTEM_ERROR;
    }

    // Create Queue Pair
    struct ibv_qp_init_attr qp_init_attr = {};
    qp_init_attr.send_cq = cq_;
    qp_init_attr.recv_cq = cq_;
    qp_init_attr.qp_type = IBV_QPT_RC;  // Reliable Connection
    qp_init_attr.cap.max_send_wr = MAX_SEND_WR;
    qp_init_attr.cap.max_recv_wr = MAX_RECV_WR;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    qp_ = ibv_create_qp(pd, &qp_init_attr);
    if (!qp_) {
        ERROR("Failed to create QP");
        return SYSTEM_ERROR;
    }
    // Modify QP to INIT state
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags =
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    ret = ibv_modify_qp(qp_, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to INIT");
        return SYSTEM_ERROR;
    }

    union ibv_gid gid;
    // get gid
    if (gidx != -1 && ibv_query_gid(ib_ctx, 1, gidx, &gid)) {
        ERROR("Failed to get GID");
        return SYSTEM_ERROR;
    }

    local_info_.qpn = qp_->qp_num;
    local_info_.psn = lrand48() & 0xffffff;
    local_info_.gid = gid;
    local_info_.lid = lid;
    local_info_.mtu = (uint32_t)active_mtu;

    INFO("gid index: {}", gidx);
    print_rdma_conn_info(&local_info_, false);
    print_rdma_conn_info(&remote_info_, true);

    // update MTU
    if (remote_info_.mtu != (uint32_t)active_mtu) {
        WARN("remote MTU: {}, local MTU: {} is not the same, update to minimal MTU",
             1 << ((uint32_t)remote_info_.mtu + 7), 1 << ((uint32_t)active_mtu + 7));
    }

    // Modify QP to RTR state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = (enum ibv_mtu)std::min((uint32_t)active_mtu, (uint32_t)remote_info_.mtu);
    attr.dest_qp_num = remote_info_.qpn;
    attr.rq_psn = remote_info_.psn;
    attr.max_dest_rd_atomic = 4;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = 0;  // RoCE v2 is used.
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;

    if (gidx == -1) {
        // IB
        attr.ah_attr.dlid = remote_info_.lid;
        attr.ah_attr.is_global = 0;
    }
    else {
        // RoCE v2
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.dgid = remote_info_.gid;
        attr.ah_attr.grh.sgid_index = gidx;
        attr.ah_attr.grh.hop_limit = 1;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ret = ibv_modify_qp(qp_, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTR: reason: {}", strerror(ret));
        return SYSTEM_ERROR;
    }

    // Modify QP to RTS state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = local_info_.psn;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
            IBV_QP_MAX_QP_RD_ATOMIC;

    ret = ibv_modify_qp(qp_, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTS");
        return SYSTEM_ERROR;
    }
    INFO("RDMA exchange done");
    rdma_connected_ = true;

    if (posix_memalign((void **)&send_buffer_, 4096, PROTOCOL_BUFFER_SIZE) != 0) {
        ERROR("Failed to allocate send buffer");
        return SYSTEM_ERROR;
    }

    send_mr_ = ibv_reg_mr(pd, send_buffer_, PROTOCOL_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr_) {
        ERROR("Failed to register MR");
        return SYSTEM_ERROR;
    }

    for (int i = 0; i < MAX_RECV_WR; i++) {
        if (posix_memalign((void **)&recv_buffer_[i], 4096, PROTOCOL_BUFFER_SIZE) != 0) {
            ERROR("Failed to allocate recv buffer");
            return SYSTEM_ERROR;
        }

        recv_mr_[i] = ibv_reg_mr(pd, recv_buffer_[i], PROTOCOL_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
        if (!recv_mr_[i]) {
            ERROR("Failed to register MR");
            return SYSTEM_ERROR;
        }

        if (prepare_recv_rdma_request(i) < 0) {
            ERROR("Failed to prepare recv rdma request");
            return SYSTEM_ERROR;
        }
    }

    if (ibv_req_notify_cq(cq_, 0)) {
        ERROR("Failed to request notify for CQ");
        return SYSTEM_ERROR;
    }

    uv_poll_init(loop, &poll_handle_, comp_channel_->fd);
    poll_handle_.data = this;
    uv_poll_start(&poll_handle_, UV_READABLE | UV_WRITABLE,
                  [](uv_poll_t *handle, int status, int events) {
                      client_t *client = static_cast<client_t *>(handle->data);
                      client->cq_poll_handle(handle, status, events);
                  });

    // Send server's RDMA connection info to client
    send_resp(FINISH, &local_info_, sizeof(local_info_));
    reset_client_read_state();
    return 0;
}

// send_resp send fixed size response to client.
void Client::send_resp(int return_code, void *buf, size_t size) {
    if (size > 0) {
        assert(buf != NULL);
    }
    uv_write_t *write_req = (uv_write_t *)malloc(sizeof(uv_write_t));

    tcp_send_buffer_ = (char *)realloc(tcp_send_buffer_, size + RETURN_CODE_SIZE);

    memcpy(tcp_send_buffer_, &return_code, RETURN_CODE_SIZE);
    memcpy(tcp_send_buffer_ + RETURN_CODE_SIZE, buf, size);
    write_req->data = this;
    uv_buf_t wbuf = uv_buf_init(tcp_send_buffer_, size + RETURN_CODE_SIZE);
    uv_write(write_req, (uv_stream_t *)handle_, &wbuf, 1, on_write);
}

int Client::sync_stream() {
    send_resp(FINISH, &remain_, sizeof(remain_));
    // Reset client state
    reset_client_read_state();
    return 0;
}

int Client::check_key(const std::string &key_to_check) {
    int ret;
    // check if the key exists and committed
    if (kv_map.count(key_to_check) > 0 && kv_map[key_to_check]->committed) {
        ret = 0;
    }
    else {
        ret = 1;
    }

    send_resp(FINISH, &ret, sizeof(ret));
    reset_client_read_state();
    return 0;
}

int Client::get_match_last_index(const GetMatchLastIndexRequest *request) {
    int left = 0, right = request->keys()->size();
    while (left < right) {
        int mid = left + (right - left) / 2;
        request->keys()->Get(mid);
        if (kv_map.count(request->keys()->Get(mid)->str())) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }
    left--;
    send_resp(FINISH, &left, sizeof(left));
    reset_client_read_state();
    return 0;
}

// return value of handle_request:
// if ret is less than 0, it is an system error, outer code will close the
// connection if ret is greater than 0, it is an application error or success
void handle_request(uv_stream_t *stream, client_t *client) {
    auto start = std::chrono::high_resolution_clock::now();
    int error_code = 0;
    int op = client->header_.op;
    // if error_code is not 0, close the connection
    switch (client->header_.op) {
        case OP_R: {
            const LocalMetaRequest *request = GetLocalMetaRequest(client->tcp_recv_buffer_);
            error_code = client->read_cache(request);
            break;
        }
        case OP_W: {
            const LocalMetaRequest *request = GetLocalMetaRequest(client->tcp_recv_buffer_);
            error_code = client->write_cache(request);
            break;
        }
        case OP_SYNC: {
            error_code = client->sync_stream();
            break;
        }
        case OP_RDMA_EXCHANGE: {
            memcpy((void *)(&client->remote_info_), client->tcp_recv_buffer_,
                   client->expected_bytes_);
            error_code = client->rdma_exchange();
            break;
        }
        case OP_CHECK_EXIST: {
            std::string key_to_check(client->tcp_recv_buffer_, client->expected_bytes_);
            INFO("check key: {}", key_to_check);
            error_code = client->check_key(key_to_check);
            break;
        }
        case OP_GET_MATCH_LAST_IDX: {
            const GetMatchLastIndexRequest *request =
                GetGetMatchLastIndexRequest(client->tcp_recv_buffer_);
            error_code = client->get_match_last_index(request);
            break;
        }
        default:
            ERROR("Invalid request");
            error_code = INVALID_REQ;
            break;
    }

    if (error_code != 0) {
        client->send_resp(error_code, NULL, 0);
        client->reset_client_read_state();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    // skip print sync log
    if (op != OP_SYNC)
        INFO("handle request {} runtime: {} ms", op_name(op), elapsed.count());
}

void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    client_t *client = (client_t *)stream->data;
    ssize_t offset = 0;

    if (nread < 0) {
        if (nread != UV_EOF)
            ERROR("Read error {}", uv_err_name(nread));
        uv_close((uv_handle_t *)stream, on_close);
        goto clean_up;
    }

    while (offset < nread) {
        switch (client->state_) {
            case READ_HEADER: {
                size_t to_copy = MIN(nread - offset, FIXED_HEADER_SIZE - client->bytes_read_);
                memcpy(((char *)&client->header_) + client->bytes_read_, buf->base + offset,
                       to_copy);
                client->bytes_read_ += to_copy;
                offset += to_copy;
                if (client->bytes_read_ == FIXED_HEADER_SIZE) {
                    DEBUG("HEADER: op: {}, body_size :{}", client->header_.op,
                          (unsigned int)client->header_.body_size);
                    if (client->header_.op == OP_R || client->header_.op == OP_W ||
                        client->header_.op == OP_CHECK_EXIST ||
                        client->header_.op == OP_GET_MATCH_LAST_IDX ||
                        client->header_.op == OP_RDMA_EXCHANGE) {
                        int ret = veryfy_header(&client->header_);
                        if (ret != 0) {
                            ERROR("Invalid header");
                            uv_close((uv_handle_t *)stream, on_close);
                            goto clean_up;
                        }
                        // prepare for reading body
                        client->expected_bytes_ = client->header_.body_size;
                        client->bytes_read_ = 0;
                        client->tcp_recv_buffer_ =
                            (char *)realloc(client->tcp_recv_buffer_, client->expected_bytes_);
                        client->state_ = READ_BODY;
                    }
                    else if (client->header_.op == OP_SYNC) {
                        handle_request(stream, client);
                    }
                }
                break;
            }

            case READ_BODY: {
                assert(client->tcp_recv_buffer_ != NULL);

                DEBUG("reading body, bytes_read: {}, expected_bytes: {}", client->bytes_read_,
                      client->expected_bytes_);
                size_t to_copy = MIN(nread - offset, client->expected_bytes_ - client->bytes_read_);

                memcpy(client->tcp_recv_buffer_ + client->bytes_read_, buf->base + offset, to_copy);
                client->bytes_read_ += to_copy;
                offset += to_copy;
                if (client->bytes_read_ == client->expected_bytes_) {
                    DEBUG("body read done, size {}", client->expected_bytes_);
                    handle_request(stream, client);
                }
                break;
            }
        }
    }
clean_up:
    free(buf->base);
}

void on_new_connection(uv_stream_t *server, int status) {
    INFO("new connection...");
    if (status < 0) {
        ERROR("New connection error {}", uv_strerror(status));
        return;
    }
    uv_tcp_t *client_handle = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(loop, client_handle);
    if (uv_accept(server, (uv_stream_t *)client_handle) == 0) {
        client_t *client = new client_t();
        // TODO: use constructor
        client->handle_ = client_handle;
        client_handle->data = client;
        client->state_ = READ_HEADER;
        client->bytes_read_ = 0;
        client->expected_bytes_ = FIXED_HEADER_SIZE;
        uv_read_start((uv_stream_t *)client_handle, alloc_buffer, on_read);
    }
    else {
        uv_close((uv_handle_t *)client_handle, NULL);
    }
}

int register_server(unsigned long loop_ptr, server_config_t config) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);

    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGBUS, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);

    // verfication
    assert(config.num_stream > 0 &&
           (config.num_stream == 1 || config.num_stream == 2 || config.num_stream == 4));

    global_config = config;

    loop = uv_default_loop();

    loop = (uv_loop_t *)loop_ptr;
    assert(loop != NULL);
    uv_tcp_init(loop, &server);
    struct sockaddr_in addr;
    uv_ip4_addr("0.0.0.0", config.service_port, &addr);

    uv_tcp_bind(&server, (const struct sockaddr *)&addr, 0);
    int r = uv_listen((uv_stream_t *)&server, 128, on_new_connection);
    if (r) {
        fprintf(stderr, "Listen error: %s\n", uv_strerror(r));
        return -1;
    }

    if (init_rdma_context(config) < 0) {
        return -1;
    }
    mm = new MM(config.prealloc_size << 30, config.minimal_allocate_size << 10, pd);

    INFO("register server done");

    return 0;
}