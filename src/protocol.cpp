#include "protocol.h"
#include <msgpack.hpp>



bool serialize_local_meta(const local_meta_t& meta, std::string& out) {
    try {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, meta);
        out.assign(sbuf.data(), sbuf.size());
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool deserialize_local_meta(const char* data, size_t size, local_meta_t& out) {
    try {
        msgpack::object_handle oh = msgpack::unpack(data, size);
        oh.get().convert(out);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}