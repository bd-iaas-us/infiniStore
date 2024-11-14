#include "protocol.h"

#include <msgpack.hpp>

std::unordered_map<char, std::string> op_map = {{OP_R, "READ"},
                                                {OP_W, "WRITE"},
                                                {OP_SYNC, "SYNC"},
                                                {OP_RDMA_EXCHANGE, "RDMA_EXCHANGE"},
                                                {OP_RDMA_WRITE, "RDMA_WRITE"},
                                                {OP_RDMA_READ, "RDMA_READ"},
                                                {OP_CHECK_EXIST, "CHECK_EXIST"},
                                                {OP_GET_MATCH_LAST_IDX, "GET_MATCH_LAST_IDX"}};
std::string op_name(char op_code) {
    auto it = op_map.find(op_code);
    if (it != op_map.end()) {
        return it->second;
    }
    return "UNKNOWN";  // 如果未找到匹配项
}

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
bool serialize_to_fixed(const T& data, char* buffer, size_t buffer_size, size_t& packed_size) {
    try {
        msgpack::sbuffer sbuffer;
        msgpack::packer<msgpack::sbuffer> packer(sbuffer);
        packer.pack(data);
        packed_size = sbuffer.size();
        if (packed_size > buffer_size) {
            return false;
        }
        std::memcpy(buffer, sbuffer.data(), packed_size);
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

template bool serialize<keys_t>(const keys_t& data, std::string& out);
template bool deserialize<keys_t>(const char* data, size_t size, keys_t& out);
template bool serialize<local_meta_t>(const local_meta_t& data, std::string& out);
template bool deserialize<local_meta_t>(const char* data, size_t size, local_meta_t& out);
template bool serialize<remote_meta_request>(const remote_meta_request& data, std::string& out);
template bool deserialize<remote_meta_request>(const char* data, size_t size,
                                               remote_meta_request& out);
template bool serialize<rdma_allocate_response>(const rdma_allocate_response& data,
                                                std::string& out);
template bool deserialize<rdma_allocate_response>(const char* data, size_t size,
                                                  rdma_allocate_response& out);
template bool serialize_to_fixed<rdma_allocate_response>(const rdma_allocate_response& data,
                                                         char* buffer, size_t buffer_size,
                                                         size_t& packed_size);
template bool serialize_to_fixed<remote_meta_request>(const remote_meta_request& data, char* buffer,
                                                      size_t buffer_size, size_t& packed_size);
