#include <cuda.h>
#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "protocol.h"
#include "utils.h"
#include <assert.h>
#include <time.h>
#include <math.h>
#include "libinfinistore.h"
#include <vector>
#include "ibv_helper.h"
#include "log.h"
#include "config.h"


Connection::~Connection() {
    DEBUG("destroying connection");

    if (cq_future.valid()) {

        stop = true;


        //create fake wr to wake up cq thread
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
        ibv_post_send(qp, &send_wr, &bad_send_wr);
        //wait thread done
        cq_future.get();
    }

    if (comp_channel) {
        ibv_destroy_comp_channel(comp_channel);
    }

    if (sock) {
        close(sock);
    }

    for (auto it = local_mr.begin(); it != local_mr.end(); it++) {
        it->second->release();
    }
    local_mr.clear();


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

int modify_qp_to_init(struct ibv_qp *qp);
int modify_qp_to_rts(connection_t *conn);
int modify_qp_to_rtr(connection_t *conn);
int exchange_conn_info(connection_t *conn);


#include <infiniband/verbs.h>
#include <atomic>
#include <stdexcept>
#include <iostream>


int init_rdma_resources(connection_t *conn, const char *dev_name) {
    // Get list of RDMA devices
    struct ibv_device **dev_list;
    struct ibv_device  *ib_dev;
    int num_devices;

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        ERROR("Failed to get RDMA devices list");
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        char *dev_name_from_list = (char*)ibv_get_device_name(dev_list[i]);
        if (strcmp(dev_name_from_list, dev_name) == 0) {
            INFO("found device {}", dev_name_from_list);
            ib_dev = dev_list[i];
            conn->ib_ctx = ibv_open_device(ib_dev);
            break;
        }
    }

    if (!conn->ib_ctx) {
        INFO("Can't find or failed to open the specified device, try to open the default device");
        conn->ib_ctx = ibv_open_device(dev_list[0]);
        if (!conn->ib_ctx) {
            ERROR("Failed to open the default device");
            return -1;
        }
    }
    ibv_free_device_list(dev_list);

    int gidx = ibv_find_sgid_type(conn->ib_ctx, 1, IBV_GID_TYPE_ROCE_V2, AF_INET);
    if (gidx < 0) {
        ERROR("Failed to find GID");
        return -1;
    }
    conn->gidx = gidx;


    union ibv_gid gid;
    //get gid
    if (ibv_query_gid(conn->ib_ctx, 1, gidx, &gid)) {
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
    conn->cq = ibv_create_cq(conn->ib_ctx, MAX_WR * 2, NULL, conn->comp_channel, 0);
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
    qp_init_attr.qp_type = IBV_QPT_RC; // Reliable Connection
    qp_init_attr.cap.max_send_wr = MAX_WR;
    qp_init_attr.cap.max_recv_wr = MAX_WR;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    conn->qp = ibv_create_qp(conn->pd, &qp_init_attr);
    if (!conn->qp) {
        ERROR("Failed to create QP, {}", strerror(errno));
        return -1;
    }

    // Modify QP to INIT state
    if (modify_qp_to_init(conn->qp)) {
        ERROR("Failed to modify QP to INIT, {}", strerror(errno));
        return -1;
    }

    // Get local connection information
    struct ibv_port_attr port_attr;
    if (ibv_query_port(conn->ib_ctx, 1, &port_attr)) {
        ERROR("Failed to query port, {}", strerror(errno));
        return -1;
    }

    conn->local_info.qpn = conn->qp->qp_num;
    conn->local_info.psn = lrand48() & 0xffffff;
    conn->local_info.gid = gid;
    DEBUG("gid index: {}", gidx);
    print_rdma_conn_info(&conn->local_info, false);
    print_rdma_conn_info(&conn->remote_info, true);
    return 0;
}


int modify_qp_to_init(struct ibv_qp *qp) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    int ret = ibv_modify_qp(qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to INIT");
        return ret;
    }
    return 0;
}

int sync_rdma(connection_t *conn) {

    std::unique_lock<std::mutex> lock(conn->mutex);
    conn->cv.wait(lock, [&conn] { return conn->rdma_inflight_count == 0; });
    return 0;
}


//assume all memory regions are registered
IBVMemoryRegion * search_mr_from_ptr(std::map<uintptr_t, IBVMemoryRegion *> &mrs, void *ptr) {
    DEBUG("searching lkey for ptr {}", ptr);
    auto it = mrs.upper_bound((uintptr_t)ptr);

    //if not found
    if (it == mrs.begin()) {
        ERROR("Failed to find lkey from ptr {}, reason 1", ptr);
        assert(false);
        return NULL;
    }
    //it could be end. this means the ptr is larger than all registered memory regions
    it --;
    if (it->first <= (uintptr_t)ptr && (uintptr_t)ptr < it->first + it->second->get_mr()->length) {
        return it->second;
    }
    //if not found, it should be an error
    ERROR("Failed to find lkey from ptr {}, reason 2", ptr);
    assert(false);
    return NULL;
}


int perform_rdma_read(connection_t *conn, uintptr_t src_buf, size_t src_size,
                      char * dst_buf, size_t dst_size, uint32_t rkey, IBVMemoryRegion *mr) {



    // Prepare RDMA read operation
    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)dst_buf;
    sge.length = dst_size;

    sge.lkey = mr->get_mr()->lkey;

    struct ibv_send_wr wr = {};
    if (conn->limited_bar1) {
        wr.wr_id = (uint64_t)mr;
        mr->add_ref();
    } else {
        wr.wr_id = (uintptr_t)conn;
    }
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = src_buf; // Obtain from server
    wr.wr.rdma.rkey = rkey;        // Obtain from server

    struct ibv_send_wr *bad_wr = NULL;
    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret) {
        ERROR("Failed to post RDMA read");
        return -1;
    }

    conn->rdma_inflight_count ++;
    DEBUG("RDMA read completed successfully");
    // RDMA read successful
    return 0;
}



int perform_rdma_write(connection_t *conn, char * src_buf, size_t src_size,
                       uintptr_t dst_buf, size_t dst_size, uint32_t rkey, IBVMemoryRegion *mr) {


    // Prepare RDMA write operation
    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)src_buf;
    sge.length = src_size;
    sge.lkey = mr->get_mr()->lkey;

    struct ibv_send_wr wr = {};

    if (conn->limited_bar1) {
        //we have to pass mr to cq_handler to deregister it
        wr.wr_id = (uint64_t)mr;
        mr->add_ref();
    } else {
        //maybe we will use request ctx in the future
        wr.wr_id = (uintptr_t)conn;
    }
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = dst_buf; // Obtain from server
    wr.wr.rdma.rkey = rkey;        // Obtain from server

    struct ibv_send_wr *bad_wr = NULL;
    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret) {
        ERROR("Failed to post RDMA write :{}" , strerror(ret));
        return -1;
    }
    conn->rdma_inflight_count ++;
    return 0;
}

void cq_handler(connection_t *conn) {
    assert(conn->comp_channel != NULL);
    while (!conn->stop) {
        struct ibv_cq *ev_cq;
        void *ev_ctx;
        int ret = ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx);
        if (ret  == 0) {
            ibv_ack_cq_events(ev_cq, 1);
            if (ibv_req_notify_cq(ev_cq, 0)) {
                ERROR("Failed to request CQ notification");
                return;
            }
            struct ibv_wc wc = {};
            while (ibv_poll_cq(conn->cq, 1, &wc) == 1) {
                if (wc.status != IBV_WC_SUCCESS) {
                    //only fake wr will use IBV_WC_SEND
                    //we use it to wake up cq thread and exit
                    if (wc.opcode == IBV_WC_SEND) {
                        return;
                    }
                    ERROR("Failed status: {}", ibv_wc_status_str(wc.status));
                    return;
                }
                if (wc.opcode == IBV_WC_RDMA_READ || wc.opcode == IBV_WC_RDMA_WRITE) {
                    conn->rdma_inflight_count --;
                    if (conn->limited_bar1) {
                        IBVMemoryRegion * mr = (IBVMemoryRegion*)wc.wr_id;
                        DEBUG("deregister mr: {}, PTR", (void*)mr);
                        conn->rdma_inflight_mr_size -= mr->release();
                    }
                } else {
                    ERROR("Unexpected opcode: {}", (int)wc.opcode);
                    return;
                }
                conn->cv.notify_all();
            }
        } else {
            //TODO: gracefull shutdown
            if (errno != EINTR) {
                ERROR("Failed to get CQ event {}", strerror(errno));
                return;
            }
        }
    }
}

int setup_rdma(connection_t *conn, client_config_t config) {
    if(init_rdma_resources(conn, config.dev_name.c_str()) < 0) {
        ERROR("Failed to initialize RDMA resources");
        delete conn;
        return -1;
    }

    // Exchange RDMA connection information with the server
    if (exchange_conn_info(conn)) {
        ERROR("Failed to exchange connection information");
        delete conn;
        return -1;
    }

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


    conn->rdma_inflight_count = 0;
    conn->stop = false;
    conn->cq_future = std::async(std::launch::async, cq_handler, conn);
    return 0;
}

int init_connection(connection_t *conn, client_config_t config)   {
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
    if(inet_pton(AF_INET, config.host_addr.data(), &serv_addr.sin_addr) <= 0) {
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
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote_info->qpn;
    attr.rq_psn = remote_info->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = 1;

    // RoCE v2
    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.dgid = remote_info->gid;
    attr.ah_attr.grh.sgid_index = conn->gidx; //local gid
    attr.ah_attr.grh.hop_limit = 1;


    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
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
    attr.sq_psn = conn->local_info.psn; // Use 0 or match with local PSN
    attr.max_rd_atomic = 1;

    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

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
    send_exact(conn->sock, &header, FIXED_HEADER_SIZE);
    send_exact(conn->sock, &conn->local_info, sizeof(rdma_conn_info_t));


    recv(conn->sock, &conn->remote_info, sizeof(rdma_conn_info_t), MSG_WAITALL);
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
    recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL);
    return return_code;
}



int rw_rdma(connection_t *conn, char op, std::vector<block_t>& blocks, int block_size, void * base_ptr, size_t ptr_region_size) {
    assert(conn != NULL);
    assert(op == OP_RDMA_READ || op == OP_RDMA_WRITE);
    assert(base_ptr != NULL);

    std::vector<std::pair<uintptr_t, uintptr_t>> mr_blocks;

    //if limited_bar1, we use temperary mrs
    std::map<uintptr_t, IBVMemoryRegion*> local_mr;

    if (conn->limited_bar1) {
        //compare already registered blocks with incoming blocks

        if (conn->rdma_inflight_mr_size + blocks.size() * block_size > conn->bar1_mem_in_mib * 1024 * 1024) {
            ERROR("Not enough BAR1 memory rdma_inflight_mr_size {} + incoming size {} > bar1 size {}", conn->rdma_inflight_mr_size, blocks.size() * block_size, conn->bar1_mem_in_mib * 1024 * 1024);
            return -1;
        }

        std::pair<unsigned long, unsigned long> cur_block = {0, 0};
        size_t cur_end = -1;
        //sort blocks by offset
        //TODO:usualy blocks is already sorted, we can optimize this.
        std::sort(blocks.begin(), blocks.end(), [](const block_t &a, const block_t &b) { return a.offset < b.offset; });

        for (auto &block : blocks) {
            if (block.offset == cur_end) {
                cur_block.second += block_size;
            } else {
                if (cur_block.second != 0) {
                    mr_blocks.push_back(cur_block);
                }
                cur_block.first = block.offset;
                cur_block.second = block.offset + block_size;
                DEBUG("cur_block: {}, {}", cur_block.first, cur_block.second);

            }
            cur_end = cur_block.second;
        }
        if (cur_block.second != 0) {
            mr_blocks.push_back(cur_block);
        }

        DEBUG("mr_blocks size: {}, blocks size: {}", mr_blocks.size(), blocks.size());

        //register mr for each block, and save it to temperary local_mr
        for (auto &mr_block : mr_blocks) {
            void *ptr = base_ptr + mr_block.first;
            IBVMemoryRegion * mr = new IBVMemoryRegion(conn->pd, ptr, mr_block.second - mr_block.first);
            local_mr[(uintptr_t)ptr] = mr;
            conn->rdma_inflight_mr_size += mr->get_mr()->length;
        }

    } else {
        //A10G or V100 has enough bar1 memory, so we can register the whole memory region
        if (conn->local_mr.find((uintptr_t)base_ptr) == conn->local_mr.end()) {
            IBVMemoryRegion * mr = new IBVMemoryRegion(conn->pd, base_ptr, ptr_region_size);
            conn->local_mr[(uintptr_t)base_ptr] = mr;
            mr->add_ref();
        } else {
            //ptr has been registered
        }

    }

    std::vector<std::string> keys;
    for (auto &block : blocks) {
        keys.push_back(block.key);
    }
    remote_meta_request request = {
        .keys = keys,
        .block_size = block_size,
    };

    std::string serialized_data;
    if (!serialize(request, serialized_data)) {
        ERROR("Failed to serialize remote meta request");
        return -1;
    }

    header_t header = {
        .magic = MAGIC,
        .op = op,
        .body_size = static_cast<unsigned int>(serialized_data.size()),
    };

    // Send header
    if (send_exact(conn->sock, &header, FIXED_HEADER_SIZE) < 0) {
        ERROR("Failed to send header");
        return -1;
    }
    // Send body
    if (send_exact(conn->sock, serialized_data.data(), serialized_data.size()) < 0) {
        ERROR("Failed to send body");
        return -1;
    }
    remote_meta_response response;
    int return_size;
    if(recv(conn->sock, &return_size, RETURN_CODE_SIZE, MSG_WAITALL) < 0) {
        ERROR("Failed to receive return size");
        return -1;
    }
    char response_data[return_size];
    if(recv(conn->sock, &response_data, return_size, MSG_WAITALL) < 0 ) {
        ERROR("Failed to receive response data");
        return -1;
    }

    if(!deserialize(response_data, return_size, response)) {
        ERROR("deserialize failed");
        return -1;
    }

    if (response.error_code != TASK_ACCEPTED) {
        ERROR("Remote operation failed {}", response.error_code);
        return -1;
    }


    if (response.blocks.size() != blocks.size()) {
        ERROR("Invalid response");
        return -1;
    }


    //if incoming blocks plus current inflight blocks exceed MAX_WR, wait
    std::unique_lock<std::mutex> lock(conn->mutex);
    conn->cv.wait(lock, [&conn, &blocks] { return conn->rdma_inflight_count + blocks.size() <= MAX_WR; });


    for (int i = 0; i < response.blocks.size(); i++) {
        DEBUG("remote response: addr: {}, rkey: {}", response.blocks[i].remote_addr, response.blocks[i].rkey);

        //request_mr could be temperary mr or registered mr
        IBVMemoryRegion *request_mr = NULL;
        if (conn->limited_bar1) {
            request_mr = search_mr_from_ptr(local_mr, (char *)base_ptr + blocks[i].offset);
        } else {
            request_mr = search_mr_from_ptr(conn->local_mr, (char *)base_ptr + blocks[i].offset);
        }
        int ret;
        if (op == OP_RDMA_WRITE) {
            ret = perform_rdma_write(conn, (char *)base_ptr + blocks[i].offset, block_size, response.blocks[i].remote_addr, block_size, response.blocks[i].rkey, request_mr);
        } else if (op == OP_RDMA_READ) {
            ret = perform_rdma_read(conn, response.blocks[i].remote_addr, block_size, (char *)base_ptr + blocks[i].offset, block_size, response.blocks[i].rkey, request_mr);
        } else {
            ERROR("Invalid operation");
            return -1;
        }
        if (ret < 0) {
            ERROR("Failed to perform RDMA operation");
            return -1;
        }
    }

    return 0;

}

int rw_local(connection_t *conn, char op, const std::vector<block_t>& blocks, int block_size, void *ptr) {
    assert(conn != NULL);
    assert(ptr != NULL);

    cudaIpcMemHandle_t ipc_handle;
    memset(&ipc_handle, 0, sizeof(cudaIpcMemHandle_t));

    CHECK_CUDA(cudaIpcGetMemHandle(&ipc_handle, ptr));

    local_meta_t meta = {
        .ipc_handle = ipc_handle,
        .block_size = block_size,
        .blocks = blocks,
    };

    std::string serialized_data;
    if (!serialize(meta, serialized_data)) {
        ERROR("Failed to serialize local meta");
        return -1;
    }

    header_t header = {
        .magic = MAGIC,
        .op = op,
        .body_size = static_cast<unsigned int>(serialized_data.size()),
    };

    // Send header
    if (send_exact(conn->sock, &header, FIXED_HEADER_SIZE) < 0) {
        ERROR("Failed to send header");
        return -1;
    }
    // Send body
    if (send_exact(conn->sock, serialized_data.data(), serialized_data.size()) < 0) {
        ERROR("Failed to send body");
        return -1;
    }

    int return_code;
    if (recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }

    if (return_code != FINISH && return_code != TASK_ACCEPTED) {
        return -1;
    }
    return 0;
}
