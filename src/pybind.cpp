#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "libinfinity.h"
#include <vector>
#include <string>
#include <tuple>
#include <iostream>

namespace py = pybind11;
extern int register_server(unsigned int loop_ptr);

int rw_local_wrapper(connection_t *conn, char op, int block_size, const std::vector<std::tuple<std::string, unsigned long>> &blocks, \
            unsigned long ptr, int device_id) {

    std::vector<block_t> c_blocks;
    for (const auto& block : blocks) {
            c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
    }
    return rw_local(conn, op, c_blocks, block_size, (void*)ptr, device_id);
}


PYBIND11_MODULE(_infinity, m) {
    py::class_<connection_t>(m, "Connection")
        .def(py::init<>())
        .def_readwrite("sock", &connection_t::sock);
    m.def("init_connection", &init_connection, "Initialize a connection");
    m.def("close_connection", &close_connection, "Close a connection");
    m.def("rw_local", &rw_local_wrapper, "Read/Write cpu memory from GPU device");
    m.def("register_server", &register_server, "register the server");
    m.def("sync_local", &sync_local, "sync the cuda stream");
    m.def("get_kvmap_len", &get_kvmap_len, "get kv map size");
}



