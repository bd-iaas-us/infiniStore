#ifndef PROTOCOL_H
#define PROTOCOL_H


#include <cuda.h>
#include <cuda_runtime.h>
#include <vector>
#include <string>
#include <msgpack.hpp>
#include <infiniband/verbs.h>


/*

Error code:
+--------------------+
| ERROR_CODE(4 bytes)|
+--------------------+

*/
#define PORT 12345

#define MAGIC 0xdeadbeef
#define MAGIC_SIZE 4

#define OP_R 'R'
#define OP_W 'W'
#define OP_SYNC 'S'
#define OP_RDMA_EXCHANGE 'E'
#define OP_SIZE 1

//error code: int
#define INVALID_REQ 400
#define FINISH 200
#define TASK_ACCEPTED 202
#define INTERNAL_ERROR 500
#define KEY_NOT_FOUND 404
#define RETRY 408
#define SYSTEM_ERROR 503


#define RETURN_CODE_SIZE sizeof(int)

typedef struct __attribute__((packed)){
    unsigned int magic;
    char op;
    unsigned int body_size;
} header_t;

typedef struct {
    std::string key;
    unsigned long offset;
    MSGPACK_DEFINE(key, offset)
} block_t;



//implement pack for ipcHandler
namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
namespace adaptor {

template <>
struct pack<cudaIpcMemHandle_t> {
    template <typename Stream>
    packer<Stream>& operator()(msgpack::packer<Stream>& o, const cudaIpcMemHandle_t& v) const {
        o.pack_bin(sizeof(cudaIpcMemHandle_t));
        o.pack_bin_body(reinterpret_cast<const char*>(&v), sizeof(cudaIpcMemHandle_t));
        return o;
    }
};

template <>
struct convert<cudaIpcMemHandle_t> {
    msgpack::object const& operator()(msgpack::object const& o, cudaIpcMemHandle_t& v) const {
        if (o.type != msgpack::type::BIN || o.via.bin.size != sizeof(cudaIpcMemHandle_t)) {
            throw msgpack::type_error();
        }
        std::memcpy(&v, o.via.bin.ptr, sizeof(cudaIpcMemHandle_t));
        return o;
    }
};


} // namespace adaptor
} // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} 

typedef struct {
    cudaIpcMemHandle_t ipc_handle;
    int block_size;
    std::vector<block_t> blocks;
    MSGPACK_DEFINE(ipc_handle, block_size, blocks)

} local_meta_t;


typedef struct rdma_conn_info_t {
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;
    //FIXME: server sends rkey and remote_addr to client
    uint32_t rkey;
    uintptr_t remote_addr;
} rdma_conn_info_t;


template <typename T>
bool serialize(const T& data, std::string& out) {
    try {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, data);
        out.assign(sbuf.data(), sbuf.size());
        return true;
    } catch (const std::exception&) {
        return false;
    }
}
template <typename T>
bool deserialize(const char* data, size_t size, T& out) {
    try {
        msgpack::object_handle oh = msgpack::unpack(data, size);
        oh.get().convert(out);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

template bool serialize<local_meta_t>(const local_meta_t& data, std::string& out);
template bool deserialize<local_meta_t>(const char* data, size_t size, local_meta_t& out);

#define FIXED_HEADER_SIZE sizeof(header_t)

#endif