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
bool deserialize(const char* data, size_t size, T& out) {
    try {
        msgpack::object_handle oh = msgpack::unpack(data, size);
        oh.get().convert(out);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

uint8_t* FixedBufferAllocator::allocate(size_t size) {
    if (offset_ + size > size_) {
        throw std::runtime_error("Buffer overflow in FixedBufferAllocator");
    }
    uint8_t* ptr = static_cast<uint8_t*>(buffer_) + offset_;
    offset_ += size;
    return ptr;
}

void FixedBufferAllocator::deallocate(uint8_t*, size_t) {
    // no-op
}

template bool serialize<keys_t>(const keys_t& data, std::string& out);
template bool deserialize<keys_t>(const char* data, size_t size, keys_t& out);
