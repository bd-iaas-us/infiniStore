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
