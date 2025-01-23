#include "libinfinistore.h"

#include <arpa/inet.h>
#include <assert.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <vector>

#include "config.h"
#include "ibv_helper.h"
#include "log.h"
#include "protocol.h"
#include "utils.h"

SendBuffer::SendBuffer(struct ibv_pd *pd, size_t size) {
    if (posix_memalign(&buffer_, 4096, PROTOCOL_BUFFER_SIZE) != 0) {
        assert(false);
    }
    mr_ = ibv_reg_mr(pd, buffer_, PROTOCOL_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
    assert(mr_ != NULL);
}

SendBuffer::~SendBuffer() {
    DEBUG("destroying send buffer");
    if (mr_) {
        ibv_dereg_mr(mr_);
    }
    if (buffer_) {
        free(buffer_);
    }
}

int Connection::post_send(struct ibv_send_wr *send_wr, struct ibv_send_wr **bad_wr) {
    std::unique_lock<std::mutex> lock(rdma_post_send_mutex);
    return ibv_post_send(qp, send_wr, bad_wr);
}

Connection::~Connection() {
    DEBUG("destroying connection");

    if (cq_future.valid()) {
        stop = true;

        // create fake wr to wake up cq thread
        ibv_req_notify_cq(cq, 0);
        struct ibv_sge sge;
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)this;
        sge.length = sizeof(*this);
        sge.lkey = 0;

        struct ibv_send_wr send_wr;
        memset(&send_wr, 0, sizeof(send_wr));
        send_wr.wr_id = (uintptr_t)this;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;

        struct ibv_send_wr *bad_send_wr;
        {
            std::unique_lock<std::mutex> lock(rdma_post_send_mutex);
            ibv_post_send(qp, &send_wr, &bad_send_wr);
        }
        // wait thread done
        cq_future.get();
    }

    if (comp_channel) {
        ibv_destroy_comp_channel(comp_channel);
    }

    if (sock) {
        close(sock);
    }

    for (auto it = local_mr.begin(); it != local_mr.end(); it++) {
        ibv_dereg_mr(it->second);
    }
    local_mr.clear();

    if (recv_mr) {
        ibv_dereg_mr(recv_mr);
    }

    if (recv_buffer) {
        free(recv_buffer);
    }

    SendBuffer *buffer;
    while (send_buffers.pop(buffer)) {
        delete buffer;
    }

    if (qp) {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_ERR;
        ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    }
    if (qp) {
        ibv_destroy_qp(qp);
    }
    if (cq) {
        ibv_destroy_cq(cq);
    }
    if (pd) {
        ibv_dealloc_pd(pd);
    }
    if (ib_ctx) {
        ibv_close_device(ib_ctx);
    }
}

int modify_qp_to_init(connection_t *conn);
int modify_qp_to_rts(connection_t *conn);
int modify_qp_to_rtr(connection_t *conn);
int exchange_conn_info(connection_t *conn);

#include <infiniband/verbs.h>

#include <atomic>
#include <iostream>
#include <stdexcept>

int init_rdma_resources(connection_t *conn, client_config_t config) {
    // Get list of RDMA devices
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
            conn->ib_ctx = ibv_open_device(ib_dev);
            break;
        }
    }

    if (!conn->ib_ctx) {
        INFO(
            "Can't find or failed to open the specified device, try to open "
            "the default device {}",
            (char *)ibv_get_device_name(dev_list[0]));
        conn->ib_ctx = ibv_open_device(dev_list[0]);
        if (!conn->ib_ctx) {
            ERROR("Failed to open the default device");
            return -1;
        }
    }
    ibv_free_device_list(dev_list);

    struct ibv_port_attr port_attr;
    conn->ib_port = config.ib_port;
    if (ibv_query_port(conn->ib_ctx, conn->ib_port, &port_attr)) {
        ERROR("Unable to query port {} attributes\n", conn->ib_port);
        return -1;
    }

    int gidx = 0;
    if ((port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND && config.link_type == "Ethernet") ||
        (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET && config.link_type == "IB")) {
        ERROR("port link layer and config link type don't match");
        return -1;
    }
    if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        gidx = -1;
    }
    else {
        gidx = ibv_find_sgid_type(conn->ib_ctx, conn->ib_port, IBV_GID_TYPE_ROCE_V2, AF_INET);
        if (gidx < 0) {
            ERROR("Failed to find GID");
            return -1;
        }
    }
    conn->lid = port_attr.lid;

    conn->gidx = gidx;

    conn->active_mtu = port_attr.active_mtu;

    union ibv_gid gid;
    // get gid
    if (conn->gidx != -1 && ibv_query_gid(conn->ib_ctx, 1, gidx, &gid)) {
        ERROR("Failed to get GID");
        return -1;
    }

    // Allocate Protection Domain
    conn->pd = ibv_alloc_pd(conn->ib_ctx);
    if (!conn->pd) {
        ERROR("Failed to allocate PD");
        return -1;
    }

    conn->comp_channel = ibv_create_comp_channel(conn->ib_ctx);
    if (!conn->comp_channel) {
        ERROR("Failed to create completion channel");
        delete conn;
        return -1;
    }

    // Create Completion Queue
    conn->cq = ibv_create_cq(conn->ib_ctx, MAX_SEND_WR + MAX_RECV_WR, NULL, conn->comp_channel, 0);
    if (!conn->cq) {
        ERROR("Failed to create CQ");
        return -1;
    }

    if (ibv_req_notify_cq(conn->cq, 0)) {
        ERROR("Failed to request CQ notification");
        delete conn;
        return -1;
    }

    // Create Queue Pair
    struct ibv_qp_init_attr qp_init_attr = {};
    qp_init_attr.send_cq = conn->cq;
    qp_init_attr.recv_cq = conn->cq;
    qp_init_attr.qp_type = IBV_QPT_RC;  // Reliable Connection
    qp_init_attr.cap.max_send_wr = MAX_SEND_WR;
    qp_init_attr.cap.max_recv_wr = MAX_RECV_WR;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    conn->qp = ibv_create_qp(conn->pd, &qp_init_attr);
    if (!conn->qp) {
        ERROR("Failed to create QP, {}", strerror(errno));
        return -1;
    }

    // Modify QP to INIT state
    if (modify_qp_to_init(conn)) {
        ERROR("Failed to modify QP to INIT, {}", strerror(errno));
        return -1;
    }

    conn->local_info.qpn = conn->qp->qp_num;
    conn->local_info.psn = lrand48() & 0xffffff;
    if (conn->gidx != -1) {
        conn->local_info.gid = gid;
        DEBUG("gid index: {}", gidx);
    }
    conn->local_info.lid = conn->lid;

    conn->local_info.mtu = (uint32_t)conn->active_mtu;

    print_rdma_conn_info(&conn->local_info, false);
    return 0;
}

int modify_qp_to_init(connection_t *conn) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = conn->ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags =
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    int ret = ibv_modify_qp(conn->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to INIT");
        return ret;
    }
    return 0;
}

int sync_rdma(connection_t *conn) {
    std::unique_lock<std::mutex> lock(conn->mutex);
    bool ret = conn->cv.wait_for(lock, std::chrono::seconds(5),
                                 [&conn] { return conn->rdma_inflight_count == 0; });

    if (!ret) {
        ERROR("timeout to sync RDMA");
        return -1;
    }
    return 0;
}

void cq_handler(connection_t *conn) {
    assert(conn->comp_channel != NULL);
    while (!conn->stop) {
        struct ibv_cq *ev_cq;
        void *ev_ctx;
        int ret = ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx);
        if (ret == 0) {
            ibv_ack_cq_events(ev_cq, 1);
            if (ibv_req_notify_cq(ev_cq, 0)) {
                ERROR("Failed to request CQ notification");
                return;
            }

            struct ibv_wc wc[10] = {};
            int num_completions;
            while ((num_completions = ibv_poll_cq(conn->cq, 10, wc)) && num_completions > 0) {
                for (int i = 0; i < num_completions; i++) {
                    if (wc[i].status != IBV_WC_SUCCESS) {
                        // only fake wr will use IBV_WC_SEND
                        // we use it to wake up cq thread and exit
                        if (wc[i].opcode == IBV_WC_SEND) {
                            return;
                        }
                        ERROR("Failed status: {}", ibv_wc_status_str(wc[i].status));
                        return;
                    }

                    if (wc[i].opcode ==
                        IBV_WC_SEND) {  // read cache/allocate msg/commit msg: request sent
                        DEBUG("read cache/allocated/commit msg request send{}, ",
                              (uintptr_t)wc[i].wr_id);
                        release_send_buffer(conn, (SendBuffer *)wc[i].wr_id);
                    }
                    else if (wc[i].opcode == IBV_WC_RECV) {  // allocate msg recved.
                        DEBUG("rdma allocated recv {}", (uintptr_t)wc[i].wr_id);
                        conn->rdma_allocate_count = 0;
                        conn->allocater_cv.notify_all();
                    }
                    else if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {  // read cache done
                        uint32_t imm_data = ntohl(wc[i].imm_data);
                        INFO("Received IMM, imm_data: {}", imm_data);
                        conn->rdma_inflight_count--;
                        conn->cv.notify_all();
                    }
                    else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {  // write cache done
                        INFO("write cache completed");
                        assert(wc[i].wr_id != 0);

                        SendBuffer *send_buffer = get_send_buffer(conn);
                        FixedBufferAllocator allocator(send_buffer->buffer_, PROTOCOL_BUFFER_SIZE);
                        FlatBufferBuilder builder(64 << 10, &allocator);
                        auto *info = reinterpret_cast<std::vector<uintptr_t> *>(wc[i].wr_id);
                        auto remote_addrs_offset = builder.CreateVector(*info);

                        auto req = CreateRemoteMetaRequest(builder, 0, 0, 0, remote_addrs_offset,
                                                           OP_RDMA_WRITE_COMMIT);
                        builder.Finish(req);

                        // send RDMA COMMIT msg to server
                        struct ibv_sge sge = {0};
                        struct ibv_send_wr wr = {0};
                        struct ibv_send_wr *bad_wr = NULL;

                        sge.addr = (uintptr_t)builder.GetBufferPointer();
                        sge.length = builder.GetSize();
                        sge.lkey = send_buffer->mr_->lkey;

                        wr.wr_id = (uintptr_t)send_buffer;
                        wr.opcode = IBV_WR_SEND;
                        wr.sg_list = &sge;
                        wr.num_sge = 1;
                        wr.send_flags = IBV_SEND_SIGNALED;
                        int ret = conn->post_send(&wr, &bad_wr);
                        if (ret) {
                            ERROR("Failed to post RDMA send :{}", strerror(ret));
                            return;
                        }
                        delete info;

                        // FIXME:
                        // notify until server commit the keys
                        // if we use the same connection, it is safe for following code to read
                        // keys. however, if we use a different connection, we need to wait for
                        // server to COMMIT the keys and send ACK back. In real world, DECODE node
                        // will try to get key when conn.sync() is done, I just cross fingers to
                        // hope that server will commit the keys before DECODE node starts.
                        conn->rdma_inflight_count--;
                        conn->cv.notify_all();
                    }
                    else {
                        ERROR("Unexpected opcode: {}", (int)wc[i].opcode);
                        return;
                    }
                }
            }
        }
        else {
            // TODO: gracefull shutdown
            if (errno != EINTR) {
                ERROR("Failed to get CQ event {}", strerror(errno));
                return;
            }
        }
    }
}

SendBuffer *get_send_buffer(connection_t *conn) {
    /*
    if send buffer list is empty,we just report error, and return NULL
    normal user should not have too many inflight requests, so we just report error
    */
    assert(!conn->send_buffers.empty());

    SendBuffer *buffer;
    assert(conn->send_buffers.pop(buffer));
    return buffer;
}

void release_send_buffer(connection_t *conn, SendBuffer *buffer) {
    conn->send_buffers.push(buffer);
}

int setup_rdma(connection_t *conn, client_config_t config) {
    if (init_rdma_resources(conn, config) < 0) {
        ERROR("Failed to initialize RDMA resources");
        delete conn;
        return -1;
    }

    // Exchange RDMA connection information with the server
    if (exchange_conn_info(conn)) {
        delete conn;
        return -1;
    }

    print_rdma_conn_info(&conn->remote_info, true);

    // Modify QP to RTR state
    if (modify_qp_to_rtr(conn)) {
        ERROR("Failed to modify QP to RTR");
        delete conn;
        return -1;
    }

    if (modify_qp_to_rts(conn)) {
        ERROR("Failed to modify QP to RTS");
        delete conn;
        return -1;
    }

    if (posix_memalign(&conn->recv_buffer, 4096, PROTOCOL_BUFFER_SIZE) != 0) {
        ERROR("Failed to allocate recv buffer");
        delete conn;
        return -1;
    }
    conn->recv_mr = ibv_reg_mr(conn->pd, conn->recv_buffer, PROTOCOL_BUFFER_SIZE,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!conn->recv_mr) {
        ERROR("Failed to register recv MR");
        delete conn;
        return -1;
    }

    /*
    This is MAX_RECV_WR not MAX_SEND_WR,
    because server also has the same number of buffers
    */
    for (int i = 0; i < MAX_RECV_WR; i++) {
        conn->send_buffers.push(new SendBuffer(conn->pd, PROTOCOL_BUFFER_SIZE));
    }

    conn->rdma_inflight_count = 0;
    conn->stop = false;
    conn->cq_future = std::async(std::launch::async, cq_handler, conn);
    return 0;
}

int init_connection(connection_t *conn, client_config_t config) {
    assert(conn != NULL);
    int sock = 0;

    struct sockaddr_in serv_addr;
    // create socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        ERROR("Failed to create socket");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(config.service_port);

    // always connect to localhost
    if (inet_pton(AF_INET, config.host_addr.data(), &serv_addr.sin_addr) <= 0) {
        ERROR("Invalid address/ Address not supported");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        ERROR("Failed to connect to server");
        return -1;
    }

    conn->sock = sock;
    return 0;
}

int modify_qp_to_rtr(connection_t *conn) {
    struct ibv_qp *qp = conn->qp;
    rdma_conn_info_t *remote_info = &conn->remote_info;

    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTR;

    // update MTU
    if (conn->remote_info.mtu != conn->active_mtu) {
        WARN("remote MTU: {}, local MTU: {} is not the same, update to minimal MTU",
             1 << ((uint32_t)conn->remote_info.mtu + 7), 1 << ((uint32_t)conn->active_mtu + 7));
    }
    attr.path_mtu =
        (enum ibv_mtu)std::min((uint32_t)conn->active_mtu, (uint32_t)conn->remote_info.mtu);

    attr.dest_qp_num = remote_info->qpn;
    attr.rq_psn = remote_info->psn;
    attr.max_dest_rd_atomic = 4;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = conn->ib_port;

    if (conn->gidx == -1) {
        // IB
        attr.ah_attr.dlid = remote_info->lid;
        attr.ah_attr.is_global = 0;
    }
    else {
        // RoCE v2
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.dgid = remote_info->gid;
        attr.ah_attr.grh.sgid_index = conn->gidx;  // local gid
        attr.ah_attr.grh.hop_limit = 1;
    }

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    int ret = ibv_modify_qp(qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTR");
        return ret;
    }
    return 0;
}

int modify_qp_to_rts(connection_t *conn) {
    struct ibv_qp *qp = conn->qp;
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = conn->local_info.psn;  // Use 0 or match with local PSN
    attr.max_rd_atomic = 1;

    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    int ret = ibv_modify_qp(qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTS");
        return ret;
    }
    return 0;
}

int exchange_conn_info(connection_t *conn) {
    header_t header = {
        .magic = MAGIC,
        .op = OP_RDMA_EXCHANGE,
        .body_size = sizeof(rdma_conn_info_t),
    };

    struct iovec iov[2];
    struct msghdr msg;

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = &conn->local_info;
    iov[1].iov_len = sizeof(rdma_conn_info_t);

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(conn->sock, &msg, 0) < 0) {
        ERROR("Failed to send local connection information");
        return -1;
    }

    int return_code = -1;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) < 0) {
        ERROR("Failed to receive return code");
        return -1;
    }

    if (return_code != FINISH) {
        ERROR("Failed to exchange connection information, return code: {}", return_code);
        return -1;
    }

    if (recv(conn->sock, &conn->remote_info, sizeof(rdma_conn_info_t), MSG_WAITALL) !=
        sizeof(rdma_conn_info_t)) {
        ERROR("Failed to receive remote connection information");
        return -1;
    }
    return 0;
}

int sync_local(connection_t *conn) {
    assert(conn != NULL);
    header_t header;
    header = {
        .magic = MAGIC,
        .op = OP_SYNC,
    };
    send_exact(conn->sock, &header, FIXED_HEADER_SIZE);

    int return_code = -1;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to sync local");
        return -1;
    }

    int inflight_syncs = 0;
    if (recv(conn->sock, &inflight_syncs, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        ERROR("Failed to receive inflight mr size");
        return -1;
    }

    return inflight_syncs;
}

int check_exist(connection_t *conn, std::string key) {
    assert(conn != NULL);
    header_t header;
    header = {.magic = MAGIC, .op = OP_CHECK_EXIST, .body_size = key.size()};

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = const_cast<void *>(static_cast<const void *>(key.data()));
    iov[1].iov_len = key.size();
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(conn->sock, &msg, 0) < 0) {
        ERROR("Failed to send header and body");
        return -1;
    }

    int return_code = 0;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to check exist");
        return -1;
    }

    int exist = 0;
    if (recv(conn->sock, &exist, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        ERROR("Failed to receive exist");
        return -1;
    }
    return exist;
}

int get_match_last_index(connection_t *conn, std::vector<std::string> keys) {
    INFO("get_match_last_index");
    assert(conn != NULL);

    FlatBufferBuilder builder(64 << 10);

    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto req = CreateGetMatchLastIndexRequest(builder, keys_offset);
    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = OP_GET_MATCH_LAST_IDX,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(conn->sock, &msg, 0) < 0) {
        ERROR("Failed to send header and body");
        return -1;
    }

    int return_code = 0;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to get match last index");
        return -1;
    }

    int last_index = -1;
    if (recv(conn->sock, &last_index, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }

    return last_index;
}

// send a message to allocate memory and return the address
int allocate_rdma(connection_t *conn, std::vector<std::string> &keys, int block_size,
                  std::vector<remote_block_t> &blocks) {
    /*
    ENCODING
    remote_meta_request req = {
        .keys = keys,
        .block_size = block_size,
        .op = OP_RDMA_ALLOCATE,
    }
    */

    SendBuffer *send_buffer = get_send_buffer(conn);

    FixedBufferAllocator allocator(send_buffer->buffer_, PROTOCOL_BUFFER_SIZE);
    FlatBufferBuilder builder(64 << 10, &allocator);
    auto keys_offset = builder.CreateVectorOfStrings(keys);

    auto req = CreateRemoteMetaRequest(builder, keys_offset, block_size, 0, 0, OP_RDMA_ALLOCATE);

    builder.Finish(req);

    // send RDMA request
    struct ibv_sge sge = {0};
    struct ibv_send_wr wr = {0};
    struct ibv_send_wr *bad_wr = NULL;

    sge.addr = (uintptr_t)builder.GetBufferPointer();
    sge.length = builder.GetSize();
    sge.lkey = send_buffer->mr_->lkey;

    wr.wr_id = (uintptr_t)send_buffer;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    int ret = conn->post_send(&wr, &bad_wr);

    if (ret) {
        ERROR("Failed to post RDMA send :{}", strerror(ret));
        return -1;
    }

    struct ibv_sge recv_sge = {0};
    struct ibv_recv_wr *bad_recv_wr = NULL;
    struct ibv_recv_wr recv_wr = {0};

    // recv all remote addresses
    recv_sge.addr = (uintptr_t)conn->recv_buffer;
    recv_sge.length = PROTOCOL_BUFFER_SIZE;
    recv_sge.lkey = conn->recv_mr->lkey;

    recv_wr = {
        .wr_id = 1234,
        .next = NULL,
        .sg_list = &recv_sge,
        .num_sge = 1,
    };
    ret = ibv_post_recv(conn->qp, &recv_wr, &bad_recv_wr);
    if (ret) {
        ERROR("Failed to post RDMA recv :{}", strerror(ret));
        return -1;
    }

    assert(conn->rdma_allocate_count == 0);
    conn->rdma_allocate_count = 1;

    std::unique_lock<std::mutex> lock(conn->mutex);
    if (!conn->allocater_cv.wait_for(lock, std::chrono::seconds(15),
                                     [&conn] { return conn->rdma_allocate_count == 0; })) {
        ERROR("timeout to allocate remote memory");
        return -1;
    }

    const RdmaAllocateResponse *resp = GetRdmaAllocateResponse(conn->recv_buffer);

    INFO("Received allocate response, #keys: {}", resp->blocks()->size());

    // copy data to std::vector, and pybind11 will pass ptr to python.
    blocks.reserve(resp->blocks()->size());

    for (const auto *block : *resp->blocks()) {
        remote_block_t remote_block = {
            .rkey = block->rkey(),
            .remote_addr = block->remote_addr(),
        };
        blocks.push_back(remote_block);
    }
    return 0;
}

int w_rdma(connection_t *conn, unsigned long *p_offsets, size_t offsets_len, int block_size,
           remote_block_t *p_remote_blocks, size_t remote_blocks_len, void *base_ptr) {
    assert(conn != NULL);
    assert(base_ptr != NULL);
    assert(p_remote_blocks != NULL);
    assert(offsets_len == remote_blocks_len);

    if (!conn->local_mr.count((uintptr_t)base_ptr)) {
        ERROR("Please register memory first");
        return -1;
    }

    INFO("w_rdma, block_size: {}, base_ptr: {}", block_size, base_ptr);
    struct ibv_mr *mr = conn->local_mr[(uintptr_t)base_ptr];

    const size_t max_wr = 16;
    struct ibv_send_wr wrs[max_wr];
    struct ibv_sge sges[max_wr];

    size_t num_wr = 0;
    for (size_t i = 0; i < remote_blocks_len; i++) {
        sges[num_wr].addr = (uintptr_t)(base_ptr + p_offsets[i]);
        sges[num_wr].length = block_size;
        sges[num_wr].lkey = mr->lkey;

        wrs[num_wr].opcode = IBV_WR_RDMA_WRITE;
        if (i == remote_blocks_len - 1) {
            // save all the remote addresses for commiting keys
            auto *info = new std::vector<uintptr_t>();
            for (size_t j = 0; j < remote_blocks_len; j++) {
                info->push_back(p_remote_blocks[j].remote_addr);
            }
            wrs[num_wr].wr_id = reinterpret_cast<uint64_t>(info);
        }
        else {
            wrs[num_wr].wr_id = 0;
        }

        wrs[num_wr].sg_list = &sges[num_wr];
        wrs[num_wr].num_sge = 1;
        wrs[num_wr].send_flags = (i == remote_blocks_len - 1) ? IBV_SEND_SIGNALED : 0;
        wrs[num_wr].wr.rdma.remote_addr = p_remote_blocks[i].remote_addr;
        wrs[num_wr].wr.rdma.rkey = p_remote_blocks[i].rkey;
        wrs[num_wr].next =
            (num_wr == max_wr - 1 || i == remote_blocks_len - 1) ? nullptr : &wrs[num_wr + 1];

        num_wr++;

        if (num_wr == max_wr || i == remote_blocks_len - 1) {
            struct ibv_send_wr *bad_wr = nullptr;
            int ret = conn->post_send(&wrs[0], &bad_wr);
            if (ret) {
                ERROR("Failed to post RDMA write {}", strerror(ret));
                return -1;
            }
            num_wr = 0;
        }
    }

    conn->rdma_inflight_count++;
    DEBUG("rdma_inflight_count: {}", conn->rdma_inflight_count.load());

    return 0;
}

int r_rdma(connection_t *conn, std::vector<block_t> &blocks, int block_size, void *base_ptr) {
    assert(conn != NULL);
    assert(base_ptr != NULL);

    if (!conn->local_mr.count((uintptr_t)base_ptr)) {
        ERROR("Please register memory first");
        return -1;
    }

    INFO("r_rdma,, block_size: {}, base_ptr: {}", block_size, base_ptr);
    struct ibv_mr *mr = conn->local_mr[(uintptr_t)base_ptr];
    assert(mr != NULL);

    // recv ACK for whole batch.
    struct ibv_sge recv_sge = {
        .addr = (uintptr_t)conn->recv_buffer,
        .length = 0,
        .lkey = conn->recv_mr->lkey,
    };
    struct ibv_recv_wr recv_wr = {
        .wr_id = 0,
        .next = NULL,
        .sg_list = &recv_sge,
        .num_sge = 1,
    };

    struct ibv_recv_wr *bad_wr_recv = NULL;

    int ret = ibv_post_recv(conn->qp, &recv_wr, &bad_wr_recv);
    if (ret) {
        ERROR("Failed to post RDMA recv :{}", strerror(ret));
    }

    std::vector<std::string> keys;
    std::vector<uintptr_t> remote_addrs;
    for (auto &block : blocks) {
        keys.push_back(block.key);
        remote_addrs.push_back((uintptr_t)(base_ptr + block.offset));
    }
    /*
    remote_meta_req = {
        .keys = keys,
        .block_size = block_size,
        .rkey = mr->rkey,
        .remote_addrs = remote_addrs,
        .op = OP_RDMA_READ,
    }
    */
    SendBuffer *send_buffer = get_send_buffer(conn);
    FixedBufferAllocator allocator(send_buffer->buffer_, PROTOCOL_BUFFER_SIZE);
    FlatBufferBuilder builder(64 << 10, &allocator);

    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto remote_addrs_offset = builder.CreateVector(remote_addrs);
    auto req = CreateRemoteMetaRequest(builder, keys_offset, block_size, mr->rkey,
                                       remote_addrs_offset, OP_RDMA_READ);

    builder.Finish(req);

    // send RDMA request
    struct ibv_sge sge = {0};
    sge.addr = (uintptr_t)builder.GetBufferPointer();
    sge.length = builder.GetSize();
    sge.lkey = send_buffer->mr_->lkey;

    struct ibv_send_wr wr = {0};
    struct ibv_send_wr *bad_wr = NULL;

    wr.wr_id = (uintptr_t)send_buffer;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    ret = conn->post_send(&wr, &bad_wr);
    if (ret) {
        ERROR("Failed to post RDMA send :{}", strerror(ret));
        return -1;
    }
    conn->rdma_inflight_count++;

    return 0;
}

int rw_local(connection_t *conn, char op, const std::vector<block_t> &blocks, int block_size,
             void *ptr, int device_id) {
    assert(conn != NULL);
    assert(ptr != NULL);

    std::vector<uint8_t> ipc_handle(sizeof(cudaIpcMemHandle_t));
    CHECK_CUDA(cudaIpcGetMemHandle((cudaIpcMemHandle_t *)ipc_handle.data(), ptr));

    /*
    local_meta_t meta = {
        .ipc_handle = ipc_handle,
        .block_size = block_size,
        .blocks = blocks,
    };
    */

    // dynamic create buffer.
    FlatBufferBuilder builder(64 << 10);

    std::vector<Offset<Block>> block_offsets;
    for (const auto &block : blocks) {
        block_offsets.push_back(
            CreateBlock(builder, builder.CreateString(block.key), block.offset));
    }

    auto req =
        CreateLocalMetaRequestDirect(builder, device_id, &ipc_handle, block_size, &block_offsets);

    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = op,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(conn->sock, &msg, 0) < 0) {
        ERROR("Failed to send header and body");
        return -1;
    }

    int return_code;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }

    if (return_code != FINISH && return_code != TASK_ACCEPTED) {
        ERROR("return code: {}", return_code);
        return -1;
    }
    return 0;
}

int register_mr(connection_t *conn, void *base_ptr, size_t ptr_region_size) {
    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, base_ptr);
    if (err == cudaSuccess) {
        if (attr.type == cudaMemoryTypeDevice) {
            INFO("try to register device memory");
        }
        else if (attr.type == cudaMemoryTypeHost || attr.type == cudaMemoryTypeUnregistered) {
            INFO("try to register host memory");
        }
        else {
            WARN("the memory type of {} is {}, I do not know if registering on RMDA will be OK",
                 base_ptr, (uint32_t)attr.type);
            return -1;
        }
    }
    else {
        ERROR("Failed to get pointer attributes: {}", cudaGetErrorString(err));
        return -1;
    }

    if (conn->local_mr.count((uintptr_t)base_ptr)) {
        WARN("this memory address is already registered!");
        ibv_dereg_mr(conn->local_mr[(uintptr_t)base_ptr]);
    }
    struct ibv_mr *mr;
    mr = ibv_reg_mr(conn->pd, base_ptr, ptr_region_size,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr) {
        ERROR("Failed to register memory regions, size: {}", ptr_region_size);
        return -1;
    }
    INFO("register mr done for base_ptr: {}, size: {}", (uintptr_t)base_ptr, ptr_region_size);
    conn->local_mr[(uintptr_t)base_ptr] = mr;
    return 0;
}
