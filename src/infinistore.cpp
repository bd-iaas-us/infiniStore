// single thread right now.
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
#include <uv.h>

#include <chrono>
#include <iostream>
#include <string>
#include <unordered_map>

#include "config.h"
#include "ibv_helper.h"
#include "log.h"
#include "mempool.h"
#include "protocol.h"
#include "utils.h"

#define BUFFER_SIZE (64 << 10)

struct PTR {
    void *ptr;
    size_t size;
    int pool_idx;
};

std::unordered_map<std::string, PTR> kv_map;
uv_loop_t *loop;
uv_tcp_t server;
// global ibv context
struct ibv_context *ib_ctx;
struct ibv_pd *pd;
struct ibv_comp_channel *comp_channel;
MM *mm;

int gidx = 0;
int lid = -1;
uint8_t ib_port = -1;
ibv_mtu active_mtu = IBV_MTU_1024;

int get_kvmap_len() { return kv_map.size(); }

typedef enum {
    READ_HEADER,
    READ_BODY,
} read_state_t;

struct Client {
    uv_tcp_t *handle = NULL;    // uv_stream_t
    read_state_t state;         // state of the client, for parsing the request
    size_t bytes_read = 0;      // bytes read so far, for parsing the request
    size_t expected_bytes = 0;  // expected size of the body
    header_t header;

    // RDMA recv buffer
    char *recv_buffer = NULL;
    struct ibv_mr *recv_mr = NULL;

    // TCP send buffer
    char *send_buffer = NULL;

    int batching_rdma_ops = 0;

    cudaStream_t cuda_stream;

    rdma_conn_info_t remote_info;
    rdma_conn_info_t local_info;

    struct ibv_cq *cq = NULL;
    struct ibv_qp *qp = NULL;
    bool rdma_connected = false;
    int gidx;  // gid index

    int remain;

    uv_poll_t poll_handle;

    struct block {
        uint32_t lkey;
        uintptr_t local_addr;
    };

    Client() = default;
    Client(const Client &) = delete;
    ~Client();

    void cq_poll_handle(uv_poll_t *handle, int status, int events);
    int write_cache(remote_meta_request &remote_meta_req);
    int read_cache(remote_meta_request &remote_meta_req);

   private:
    void send_rdma_resp();
};
typedef struct Client client_t;

Client::~Client() {
    DEBUG("free client resources");

    if (poll_handle.data) {
        uv_poll_stop(&poll_handle);
    }
    if (handle) {
        free(handle);
        handle = NULL;
    }
    if (recv_buffer) {
        free(recv_buffer);
        recv_buffer = NULL;
    }
    cudaStreamDestroy(cuda_stream);
    INFO("destroy cuda stream");
    if (qp) {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_ERR;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
            ERROR("Failed to modify QP to ERR state");
        }
    }
    if (qp) {
        ibv_destroy_qp(qp);
        qp = NULL;
        INFO("QP destroyed");
    }
}

void Client::send_rdma_resp() {
    // send a empty message to wake up the cq_handler
    struct ibv_send_wr wr = {.wr_id = (uintptr_t)this,
                             .sg_list = NULL,
                             .num_sge = 0,
                             .opcode = IBV_WR_SEND,
                             .send_flags = IBV_SEND_SIGNALED};
    struct ibv_send_wr *bad_wr = NULL;
    if (ibv_post_send(qp, &wr, &bad_wr)) {
        ERROR("Failed to post ACK");
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

    if (ibv_get_cq_event(comp_channel, &cq, &cq_context) != 0) {
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
                remote_meta_request request;
                if (!deserialize(recv_buffer, wc.byte_len, request)) {
                    ERROR("Failed to deserialize remote meta request");
                    continue;
                }
                INFO("Received remote meta request, #keys: {}, block size {}", request.keys.size(),
                     request.block_size);
                switch (request.op) {
                    case OP_RDMA_READ:
                        read_cache(request);
                        break;
                    case OP_RDMA_WRITE:
                        write_cache(request);
                        break;
                    default:
                        ERROR("Unexpected request op: {}", request.op);
                        break;
                }
                struct ibv_sge sge = {0};
                struct ibv_recv_wr rwr = {0};
                struct ibv_recv_wr *bad_wr = NULL;
                sge.addr = (uintptr_t)recv_buffer;
                sge.length = BUFFER_SIZE;
                sge.lkey = recv_mr->lkey;

                rwr.wr_id = (uint64_t)recv_buffer;
                rwr.next = NULL;
                rwr.sg_list = &sge;
                rwr.num_sge = 1;
                if (ibv_post_recv(qp, &rwr, &bad_wr)) {
                    ERROR("Failed to post receive, {}");
                    return;
                }
            }
            else if (wc.opcode == IBV_WC_RDMA_READ) {  // write cache: multple RDMA read done
                DEBUG("write cache: multple RDMA read done");
                batching_rdma_ops--;
                if (batching_rdma_ops == 0) {
                    DEBUG("write cache:All RDMA read done");
                    send_rdma_resp();
                }
            }
            else if (wc.opcode == IBV_WC_SEND) {  // write cache: ACK received
                INFO("write cache ACK received");
            }
            else {
                ERROR("Unexpected wc opcode: {}", wc.opcode);
            }
        }
        else {
            ERROR("CQ error: {}, {}", ibv_wc_status_str(wc.status), wc.wr_id);
        }
    }
}

int Client::read_cache(remote_meta_request &remote_meta_req) {
    INFO("do rdma read...");
    if (remote_meta_req.keys.size() != remote_meta_req.remote_addrs.size()) {
        ERROR("keys size and remote_addrs size mismatch");
        return -1;
    }
    // std::vector<block> blocks;
    // blocks.reserve(remote_meta_req.keys.size());

    // for (const auto &key : remote_meta_req.keys) {
    //     auto it = kv_map.find(key);
    //     if (it == kv_map.end()) {
    //         ERROR("Key not found: {}", key);
    //         return -1;
    //     }
    //     const PTR &ptr = it->second;
    //     DEBUG("rkey: {}, local_addr: {}, size : {}", mm->get_lkey(ptr.pool_idx),
    //     (uintptr_t)ptr.ptr,
    //           ptr.size);
    //     blocks.push_back({.lkey = mm->get_lkey(ptr.pool_idx), .local_addr = (uintptr_t)ptr.ptr});
    // }

    for (int i = 0; i < remote_meta_req.keys.size(); i++) {
        auto it = kv_map.find(remote_meta_req.keys[i]);
        if (it == kv_map.end()) {
            ERROR("Key not found: {}", remote_meta_req.keys[i]);
            return -1;
        }
        const PTR &ptr = it->second;

        struct ibv_sge sge = {0};
        sge.addr = (uintptr_t)ptr.ptr;
        sge.length = remote_meta_req.block_size;
        sge.lkey = mm->get_lkey(ptr.pool_idx);

        struct ibv_send_wr wr = {0};
        wr.wr_id = (uint64_t)ptr.ptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = 0;
        wr.next = NULL;
        wr.wr.rdma.remote_addr = remote_meta_req.remote_addrs[i];
        wr.wr.rdma.rkey = remote_meta_req.rkey;
        if (i == remote_meta_req.keys.size() - 1) {
            wr.imm_data = htonl(100);
            wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        }
        else {
            wr.opcode = IBV_WR_RDMA_WRITE;
        }
        struct ibv_send_wr *bad_wr = NULL;
        int ret = ibv_post_send(qp, &wr, &bad_wr);
        if (ret) {
            ERROR("Failed to post RDMA write");
            return -1;
        }
    }
}

int Client::write_cache(remote_meta_request &remote_meta_req) {
    INFO("do write_cache...");

    if (remote_meta_req.keys.size() != remote_meta_req.remote_addrs.size()) {
        ERROR("keys size and remote_addrs size mismatch");
        return -1;
    }

    std::vector<block> blocks;
    blocks.reserve(remote_meta_req.keys.size());

    // allocate memory
    for (const auto &key : remote_meta_req.keys) {
        void *h_dst;
        int pool_idx;
        h_dst = mm->allocate(remote_meta_req.block_size, &pool_idx);
        // FIXME: only one h_dst is sent
        if (h_dst == NULL) {
            ERROR("Failed to allocate host memory");
            return SYSTEM_ERROR;
        }
        auto ptr = PTR{.ptr = h_dst, .size = remote_meta_req.block_size, .pool_idx = pool_idx};
        // save to the map
        kv_map[key] = ptr;
        DEBUG("rkey: {}, local_addr: {}, size : {}", mm->get_lkey(pool_idx), (uintptr_t)h_dst,
              remote_meta_req.block_size);

        blocks.push_back({.lkey = mm->get_lkey(ptr.pool_idx), .local_addr = (uintptr_t)ptr.ptr});
    }

    // Prepare RDMA read operation for write_cache
    for (int i = 0; i < remote_meta_req.keys.size(); i++) {
        struct ibv_sge sge = {};
        sge.addr = blocks[i].local_addr;
        sge.length = remote_meta_req.block_size;
        sge.lkey = blocks[i].lkey;

        struct ibv_send_wr wr = {0};
        wr.wr_id = 1234;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        if (i == remote_meta_req.keys.size() - 1) {
            wr.send_flags = IBV_SEND_SIGNALED;
        }
        else {
            wr.send_flags = 0;
        }
        wr.wr.rdma.remote_addr = remote_meta_req.remote_addrs[i];
        wr.wr.rdma.rkey = remote_meta_req.rkey;
        struct ibv_send_wr *bad_wr = NULL;
        int ret = ibv_post_send(qp, &wr, &bad_wr);
        if (ret) {
            ERROR("Failed to post RDMA read");
            return -1;
        }
    }
    batching_rdma_ops += 1;
    return 0;
}

// send response to client through TCP
void send_resp(client_t *client, int return_code, void *buf, size_t size);

typedef struct {
    client_t *client;
    void *d_ptr;
} wqueue_data_t;

// FIXME:
void reset_client_read_state(client_t *client) {
    client->state = READ_HEADER;
    client->bytes_read = 0;
    client->expected_bytes = FIXED_HEADER_SIZE;
    memset(&client->header, 0, sizeof(header_t));

    // keep the recv_buffer as it is
    memset(client->recv_buffer, 0, client->expected_bytes);
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

void wait_for_ipc_close_completion(uv_work_t *req) {
    wqueue_data_t *wqueue_data = (wqueue_data_t *)req->data;
    CHECK_CUDA(cudaIpcCloseMemHandle(wqueue_data->d_ptr));
    INFO("wait_for_ipc_close_completion done");
}

void after_ipc_close_completion(uv_work_t *req, int status) {
    wqueue_data_t *wqueue_data = (wqueue_data_t *)req->data;
    wqueue_data->client->remain--;
    INFO("after_ipc_close_completion done");
    delete wqueue_data;
    delete req;
}

int read_cache(client_t *client, local_meta_t &meta) {
    const header_t *header = &client->header;
    void *d_ptr;

    assert(header != NULL);
    // TODO: check device_id

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, meta.ipc_handle, cudaIpcMemLazyEnablePeerAccess));

    for (auto &block : meta.blocks) {
        if (kv_map.count(block.key) == 0) {
            std::cout << "Key not found: " << block.key << std::endl;
            CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
            send_resp(client, KEY_NOT_FOUND, NULL, 0);
            return 0;
        }

        // key found
        // std::cout << "Key found: " << block.key << std::endl;
        void *h_src = kv_map[block.key].ptr;
        if (h_src == NULL) {
            send_resp(client, KEY_NOT_FOUND, NULL, 0);
            return 0;
        }
        // push the host cpu data to local device
        CHECK_CUDA(cudaMemcpyAsync((char *)d_ptr + block.offset, h_src, meta.block_size,
                                   cudaMemcpyHostToDevice, client->cuda_stream));
    }
    client->remain++;
    wqueue_data_t *wqueue_data = new wqueue_data_t();
    wqueue_data->client = client;
    wqueue_data->d_ptr = d_ptr;
    uv_work_t *req = new uv_work_t();
    req->data = (void *)wqueue_data;
    uv_queue_work(loop, req, wait_for_ipc_close_completion, after_ipc_close_completion);

    send_resp(client, TASK_ACCEPTED, NULL, 0);
    reset_client_read_state(client);
    return 0;
}

int write_cache(client_t *client, local_meta_t &meta) {
    // allocate host memory
    void *d_ptr;
    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, meta.ipc_handle, cudaIpcMemLazyEnablePeerAccess));

    for (auto &block : meta.blocks) {
        // pull data from local device to CPU host
        void *h_dst;
        int pool_idx;
        h_dst = mm->allocate(meta.block_size, &pool_idx);
        if (h_dst == NULL) {
            ERROR("Failed to allocat host memroy");
            CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
            return SYSTEM_ERROR;
        }
        // how to deal with memory overflow?
        // pull data from local device to CPU host
        CHECK_CUDA(cudaMemcpyAsync(h_dst, (char *)d_ptr + block.offset, meta.block_size,
                                   cudaMemcpyDeviceToHost, client->cuda_stream));
        kv_map[block.key] = {.ptr = h_dst, .size = meta.block_size, .pool_idx = pool_idx};
    }
    client->remain++;
    wqueue_data_t *wqueue_data = new wqueue_data_t();
    wqueue_data->client = client;
    wqueue_data->d_ptr = d_ptr;
    uv_work_t *req = new uv_work_t();
    req->data = (void *)wqueue_data;
    uv_queue_work(loop, req, wait_for_ipc_close_completion, after_ipc_close_completion);

    int return_code = TASK_ACCEPTED;
    send_resp(client, TASK_ACCEPTED, NULL, 0);

    reset_client_read_state(client);
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
    comp_channel = ibv_create_comp_channel(ib_ctx);
    if (!comp_channel) {
        ERROR("Failed to create completion channel");
        return -1;
    }
    return 0;
}

int rdma_exchange(client_t *client) {
    INFO("do rdma exchange...");

    int ret;

    if (client->rdma_connected == true) {
        ERROR("RDMA already connected");
        return SYSTEM_ERROR;
    }

    // RDMA setup if not already done
    assert(comp_channel != NULL);

    client->cq = ibv_create_cq(ib_ctx, MAX_WR * 2, NULL, comp_channel, 0);
    if (!client->cq) {
        ERROR("Failed to create CQ");
        return SYSTEM_ERROR;
    }

    // Create Queue Pair
    struct ibv_qp_init_attr qp_init_attr = {};
    qp_init_attr.send_cq = client->cq;
    qp_init_attr.recv_cq = client->cq;
    qp_init_attr.qp_type = IBV_QPT_RC;  // Reliable Connection
    qp_init_attr.cap.max_send_wr = MAX_WR;
    qp_init_attr.cap.max_recv_wr = MAX_WR;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    client->qp = ibv_create_qp(pd, &qp_init_attr);
    if (!client->qp) {
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

    ret = ibv_modify_qp(client->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to INIT");
        return SYSTEM_ERROR;
    }

    client->gidx = gidx;

    union ibv_gid gid;
    // get gid
    if (gidx != -1 && ibv_query_gid(ib_ctx, 1, gidx, &gid)) {
        ERROR("Failed to get GID");
        return -1;
    }

    client->local_info.qpn = client->qp->qp_num;
    client->local_info.psn = lrand48() & 0xffffff;
    client->local_info.gid = gid;
    client->local_info.lid = lid;

    INFO("gid index: {}", client->gidx);
    print_rdma_conn_info(&client->local_info, false);

    // Modify QP to RTR state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = active_mtu;  // FIXME: hard coded
    attr.dest_qp_num = client->remote_info.qpn;
    attr.rq_psn = client->remote_info.psn;
    attr.max_dest_rd_atomic = 4;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = 0;  // RoCE v2 is used.
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;

    if (gidx == -1) {
        // IB
        attr.ah_attr.dlid = client->remote_info.lid;
        attr.ah_attr.is_global = 0;
    }
    else {
        // RoCE v2
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.dgid = client->remote_info.gid;
        attr.ah_attr.grh.sgid_index = client->gidx;
        attr.ah_attr.grh.hop_limit = 1;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ret = ibv_modify_qp(client->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTR");
        return SYSTEM_ERROR;
    }

    // Modify QP to RTS state
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = client->local_info.psn;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
            IBV_QP_MAX_QP_RD_ATOMIC;

    ret = ibv_modify_qp(client->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTS");
        return SYSTEM_ERROR;
    }
    INFO("RDMA exchange done");
    client->rdma_connected = true;

    client->recv_buffer = (char *)realloc(client->recv_buffer, BUFFER_SIZE);
    client->recv_mr = ibv_reg_mr(pd, client->recv_buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!client->recv_mr) {
        ERROR("Failed to register MR");
        return SYSTEM_ERROR;
    }

    // ready to receive RDMA_SEND
    struct ibv_sge sge = {0};
    struct ibv_recv_wr rwr = {0};

    struct ibv_recv_wr *bad_wr = NULL;

    sge.addr = (uintptr_t)client->recv_buffer;
    sge.length = BUFFER_SIZE;
    sge.lkey = client->recv_mr->lkey;

    rwr.wr_id = (uint64_t)client->recv_buffer;
    rwr.next = NULL;
    rwr.sg_list = &sge;
    rwr.num_sge = 1;

    if (ibv_post_recv(client->qp, &rwr, &bad_wr)) {
        ERROR("Failed to post receive, {}");
        return SYSTEM_ERROR;
    }

    if (ibv_req_notify_cq(client->cq, 0)) {
        ERROR("Failed to request notify for CQ");
        return -1;
    }

    uv_poll_init(loop, &client->poll_handle, comp_channel->fd);
    client->poll_handle.data = client;
    uv_poll_start(&client->poll_handle, UV_READABLE | UV_WRITABLE,
                  [](uv_poll_t *handle, int status, int events) {
                      client_t *client = static_cast<client_t *>(handle->data);
                      client->cq_poll_handle(handle, status, events);
                  });

    // Send server's RDMA connection info to client
    send_resp(client, FINISH, &client->local_info, sizeof(client->local_info));
    reset_client_read_state(client);
    return 0;
}

// send_resp send fixed size response to client.
void send_resp(client_t *client, int return_code, void *buf, size_t size) {
    if (size > 0) {
        assert(buf != NULL);
    }
    uv_write_t *write_req = (uv_write_t *)malloc(sizeof(uv_write_t));

    client->send_buffer = (char *)realloc(client->send_buffer, size + RETURN_CODE_SIZE);

    memcpy(client->send_buffer, &return_code, RETURN_CODE_SIZE);
    memcpy(client->send_buffer + RETURN_CODE_SIZE, buf, size);
    write_req->data = client;
    uv_buf_t wbuf = uv_buf_init(client->send_buffer, size + RETURN_CODE_SIZE);
    uv_write(write_req, (uv_stream_t *)client->handle, &wbuf, 1, on_write);
}

int sync_stream(client_t *client) {
    send_resp(client, FINISH, &client->remain, sizeof(client->remain));
    // Reset client state
    reset_client_read_state(client);
    return 0;
}

int check_key(client_t *client, std::string &key_to_check) {
    int ret = kv_map.count(key_to_check) ? 0 : 1;
    send_resp(client, FINISH, &ret, sizeof(ret));
    reset_client_read_state(client);
    return 0;
}

int get_match_last_index(client_t *client, keys_t &keys_meta) {
    int left = 0, right = keys_meta.keys.size();
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (kv_map.count(keys_meta.keys[mid])) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }
    left--;
    send_resp(client, FINISH, &left, sizeof(left));
    reset_client_read_state(client);
    return 0;
}

// return value of handle_request:
// if ret is less than 0, it is an system error, outer code will close the
// connection if ret is greater than 0, it is an application error or success
void handle_request(uv_stream_t *stream, client_t *client) {
    auto start = std::chrono::high_resolution_clock::now();
    int error_code = 0;
    int op = client->header.op;
    // if error_code is not 0, close the connection
    switch (client->header.op) {
        case OP_R: {
            local_meta_t local_meta;

            if (!deserialize(client->recv_buffer, client->expected_bytes, local_meta)) {
                ERROR("Failed to deserialize local meta");
                error_code = SYSTEM_ERROR;
                break;
            }
            error_code = read_cache(client, local_meta);
            break;
        }
        case OP_W: {
            local_meta_t local_meta;

            if (!deserialize(client->recv_buffer, client->expected_bytes, local_meta)) {
                ERROR("Failed to deserialize local meta");
                error_code = SYSTEM_ERROR;
                break;
            }
            error_code = write_cache(client, local_meta);
            break;
        }
        case OP_SYNC: {
            error_code = sync_stream(client);
            break;
        }
        case OP_RDMA_EXCHANGE: {
            memcpy((void *)(&client->remote_info), client->recv_buffer, client->expected_bytes);
            error_code = rdma_exchange(client);
            break;
        }
        case OP_CHECK_EXIST: {
            std::string key_to_check(client->recv_buffer, client->expected_bytes);
            error_code = check_key(client, key_to_check);
            break;
        }
        case OP_GET_MATCH_LAST_IDX: {
            keys_t keys_meta;
            if (!deserialize(client->recv_buffer, client->expected_bytes, keys_meta)) {
                ERROR("Failed to deserialize keys meta");
                error_code = SYSTEM_ERROR;
                break;
            }
            error_code = get_match_last_index(client, keys_meta);
            break;
        }
        default:
            ERROR("Invalid request");
            error_code = INVALID_REQ;
            break;
    }

    if (error_code != 0) {
        send_resp(client, error_code, NULL, 0);
        reset_client_read_state(client);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
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
        switch (client->state) {
            case READ_HEADER: {
                size_t to_copy = MIN(nread - offset, FIXED_HEADER_SIZE - client->bytes_read);
                memcpy(((char *)&client->header) + client->bytes_read, buf->base + offset, to_copy);
                client->bytes_read += to_copy;
                offset += to_copy;
                if (client->bytes_read == FIXED_HEADER_SIZE) {
                    DEBUG("HEADER: op: {}, body_size :{}", client->header.op,
                          (unsigned int)client->header.body_size);
                    if (client->header.op == OP_R || client->header.op == OP_W ||
                        client->header.op == OP_CHECK_EXIST ||
                        client->header.op == OP_GET_MATCH_LAST_IDX ||
                        client->header.op == OP_RDMA_EXCHANGE) {
                        int ret = veryfy_header(&client->header);
                        if (ret != 0) {
                            ERROR("Invalid header");
                            uv_close((uv_handle_t *)stream, on_close);
                            goto clean_up;
                        }
                        // prepare for reading body
                        client->expected_bytes = client->header.body_size;
                        client->bytes_read = 0;
                        client->recv_buffer =
                            (char *)realloc(client->recv_buffer, client->expected_bytes);
                        client->state = READ_BODY;
                    }
                    else if (client->header.op == OP_SYNC) {
                        handle_request(stream, client);
                    }
                }
                break;
            }

            case READ_BODY: {
                assert(client->recv_buffer != NULL);

                DEBUG("reading body, bytes_read: {}, expected_bytes: {}", client->bytes_read,
                      client->expected_bytes);
                size_t to_copy = MIN(nread - offset, client->expected_bytes - client->bytes_read);

                memcpy(client->recv_buffer + client->bytes_read, buf->base + offset, to_copy);
                client->bytes_read += to_copy;
                offset += to_copy;
                if (client->bytes_read == client->expected_bytes) {
                    DEBUG("body read done, size {}", client->expected_bytes);
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
        CHECK_CUDA(cudaStreamCreate(&client->cuda_stream));
        client->handle = client_handle;
        client_handle->data = client;
        client->state = READ_HEADER;
        client->bytes_read = 0;
        client->expected_bytes = FIXED_HEADER_SIZE;
        client->recv_buffer = NULL;
        uv_read_start((uv_stream_t *)client_handle, alloc_buffer, on_read);
    }
    else {
        uv_close((uv_handle_t *)client_handle, NULL);
    }
}

void signal_handler(int signum) {
    void *array[10];
    size_t size;
    if (signum == SIGSEGV) {
        ERROR("Caught SIGSEGV: segmentation fault");
        size = backtrace(array, 10);
        // print signum's name
        ERROR("Error: signal {}", signum);
        // backtrace_symbols_fd(array, size, STDERR_FILENO);
        // write backtrace_symbols_fd to log
        char **strings = backtrace_symbols(array, size);
        if (strings == NULL) {
            ERROR("Failed to get backtrace");
            exit(1);
        }
        for (size_t i = 0; i < size; i++) {
            ERROR("{}", strings[i]);
        }
        exit(1);
    }
    else {
        INFO("Caught signal {}", signum);
        // TODO: gracefully shutdown
        if (loop) {
            uv_stop(loop);
        }
        exit(0);
    }
}

int register_server(unsigned long loop_ptr, server_config_t config) {
    signal(SIGSEGV, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

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
    mm = new MM(config.prealloc_size << 30, 32 << 10, pd);

    INFO("register server done");

    return 0;
}
