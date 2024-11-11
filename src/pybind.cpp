#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include "config.h"
#include "libinfinistore.h"
#include "log.h"

namespace py = pybind11;
extern int register_server(unsigned long loop_ptr, server_config_t config);

int rw_local_wrapper(connection_t *conn, char op,
                     const std::vector<std::tuple<std::string, unsigned long>> &blocks,
                     int block_size, uintptr_t ptr) {
    std::vector<block_t> c_blocks;
    for (const auto &block : blocks) {
        c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
    }
    return rw_local(conn, op, c_blocks, block_size, (void *)ptr);
}

int rw_rdma_wrapper(connection_t *conn, char op,
                    const std::vector<std::tuple<std::string, unsigned long>> &blocks,
                    int block_size, uintptr_t ptr) {
    std::vector<block_t> c_blocks;
    for (const auto &block : blocks) {
        c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
    }
    return rw_rdma(conn, op, c_blocks, block_size, (void *)ptr);
}

int register_mr_wrapper(connection_t *conn, uintptr_t ptr, size_t ptr_region_size) {
    return register_mr(conn, (void *)ptr, ptr_region_size);
}

PYBIND11_MODULE(_infinistore, m) {
    // client side
    py::class_<client_config_t>(m, "ClientConfig")
        .def(py::init<>())
        .def_readwrite("service_port", &client_config_t::service_port)
        .def_readwrite("log_level", &client_config_t::log_level)
        .def_readwrite("dev_name", &client_config_t::dev_name)
        .def_readwrite("ib_port", &client_config_t::ib_port)
        .def_readwrite("link_type", &client_config_t::link_type)
        .def_readwrite("host_addr", &client_config_t::host_addr);

    py::class_<connection_t>(m, "Connection").def(py::init<>());

    m.def("init_connection", &init_connection, "Initialize a connection");
    m.def("rw_local", &rw_local_wrapper, "Read/Write cpu memory from GPU device");
    m.def("rw_rdma", &rw_rdma_wrapper, "Read/Write remote memory");
    m.def("sync_local", &sync_local, "sync the cuda stream");
    m.def("setup_rdma", &setup_rdma, "setup rdma connection");
    m.def("sync_rdma", &sync_rdma, "sync the remote server");
    m.def("check_exist", &check_exist, "check if the key exists in the store");
    m.def("get_match_last_index", &get_match_last_index,
          "get the last index of a key list which is in the store");
    m.def("register_mr", &register_mr_wrapper, "register memory region");

    // server side
    py::class_<server_config_t>(m, "ServerConfig")
        .def(py::init<>())
        .def_readwrite("service_port", &ServerConfig::service_port)
        .def_readwrite("log_level", &ServerConfig::log_level)
        .def_readwrite("dev_name", &ServerConfig::dev_name)
        .def_readwrite("ib_port", &ServerConfig::ib_port)
        .def_readwrite("link_type", &ServerConfig::link_type)
        .def_readwrite("prealloc_size", &ServerConfig::prealloc_size);
    m.def("get_kvmap_len", &get_kvmap_len, "get kv map size");
    m.def("register_server", &register_server, "register the server");

    // //both side
    m.def("log_msg", &log_msg, "log");
    m.def("set_log_level", &set_log_level, "set log level");
}
