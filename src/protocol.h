#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <cuda.h>
#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <string>
#include <vector>

#include "flatbuffers/flatbuffers.h"
// RDMA protocols
#include "allocate_response_generated.h"
#include "meta_request_generated.h"

// local TCP protocols
#include "get_match_last_index_generated.h"
#include "local_meta_request_generated.h"

using namespace flatbuffers;

// this number should be big for lots of RMDA_WRITE requests
#define MAX_SEND_WR 8192

// this is only used for recving RDMA_SEND or IMM data. this should be bigger than max layers of
// model.
#define MAX_RECV_WR 64

// how many RDMA write requests can be outstanding, this should be bigger than MAX_WR_BATCH and less
// than MAX_SEND_WR
#define MAX_RDMA_WRITE_WR 4096

// every MAX_WR_BATCH RDMA write requests will have a RDMA_SIGNAL
#define MAX_WR_BATCH 32

#define MAGIC 0xdeadbeef
#define MAGIC_SIZE 4

#define OP_R 'R'
#define OP_W 'W'
#define OP_SYNC 'S'
#define OP_RDMA_EXCHANGE 'E'
#define OP_RDMA_ALLOCATE 'D'
#define OP_RDMA_READ 'A'
#define OP_RDMA_WRITE_COMMIT 'T'
#define OP_CHECK_EXIST 'C'
#define OP_GET_MATCH_LAST_IDX 'M'
#define OP_SIZE 1
// please add op name in protocol.cpp

std::string op_name(char op);

// error code: int
#define INVALID_REQ 400
#define FINISH 200
#define TASK_ACCEPTED 202
#define INTERNAL_ERROR 500
#define KEY_NOT_FOUND 404
#define RETRY 408
#define SYSTEM_ERROR 503
#define OUT_OF_MEMORY 507

#define RETURN_CODE_SIZE sizeof(int)

#define PROTOCOL_BUFFER_SIZE (4 << 20)  // 4M could be enough

typedef struct __attribute__((packed)) {
    unsigned int magic;
    char op;
    unsigned int body_size;
} header_t;

// remote_block_t is used to to talk to PYTHON layer. not used in RDMA/TCP layer.
typedef struct {
    uint32_t rkey;
    uintptr_t remote_addr;
} remote_block_t;

// block_t is used to to talk to PYTHON layer. not used in RDMA/TCP layer.
typedef struct {
    std::string key;
    unsigned long offset;
} block_t;

typedef struct __attribute__((packed)) rdma_conn_info_t {
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;  // RoCE v2
    uint16_t lid;       // IB
    uint32_t mtu;       // peers should have the same mtu
} rdma_conn_info_t;

#define FIXED_HEADER_SIZE sizeof(header_t)

class FixedBufferAllocator : public Allocator {
   public:
    FixedBufferAllocator(void* buffer, size_t size) : buffer_(buffer), size_(size), offset_(0) {}

    uint8_t* allocate(size_t size) override;
    void deallocate(uint8_t*, size_t) override;

   private:
    void* buffer_;
    size_t size_;
    size_t offset_;
};

const RemoteBlock FAKE_REMOTE_BLOCK = RemoteBlock(0, 0);
bool is_fake_remote_block(remote_block_t& block);

#endif
