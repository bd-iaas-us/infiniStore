#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>
#include <memory>
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
                     int block_size, uintptr_t ptr, int device_id) {
    std::vector<block_t> c_blocks;
    for (const auto &block : blocks) {
        c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
    }
    return rw_local(conn, op, c_blocks, block_size, (void *)ptr, device_id);
}

int r_rdma_wrapper(connection_t *conn,
                   const std::vector<std::tuple<std::string, unsigned long>> &blocks,
                   int block_size, uintptr_t ptr) {
    std::vector<block_t> c_blocks;
    for (const auto &block : blocks) {
        c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
    }
    return r_rdma(conn, c_blocks, block_size, (void *)ptr);
}

int w_rdma_wrapper(
    connection_t *conn,
    py::array_t<unsigned long, py::array::c_style | py::array::forcecast> offsets, int block_size,
    py::array_t<remote_block_t, py::array::c_style | py::array::forcecast> remote_blocks,
    uintptr_t base_ptr) {
    py::buffer_info block_buf = remote_blocks.request();
    py::buffer_info offset_buf = offsets.request();

    assert(block_buf.ndim == 1);
    assert(offset_buf.ndim == 1);

    remote_block_t *p_remote_blocks = static_cast<remote_block_t *>(block_buf.ptr);
    unsigned long *p_offsets = static_cast<unsigned long *>(offset_buf.ptr);
    size_t remote_blocks_len = block_buf.shape[0];
    size_t offsets_len = offset_buf.shape[0];
    return w_rdma(conn, p_offsets, offsets_len, block_size, p_remote_blocks, remote_blocks_len,
                  (void *)base_ptr);
}

// See https://github.com/pybind/pybind11/issues/1042#issuecomment-642215028
// as_pyarray is a helper function to convert a C++ sequence to a numpy array and zero-copy
template <typename Sequence>
inline py::array_t<typename Sequence::value_type> as_pyarray(Sequence &&seq) {
    // Move entire object to heap. Memory handled via Python capsule
    Sequence *seq_ptr = new Sequence(std::move(seq));
    // Capsule shall delete sequence object when done
    auto capsule = py::capsule(seq_ptr, [](void *p) { delete reinterpret_cast<Sequence *>(p); });

    return py::array(static_cast<py::ssize_t>(seq_ptr->size()),  // shape of array
                     seq_ptr->data(),  // c-style contiguous strides for Sequence
                     capsule           // numpy array references this parent
    );
}

py::array allocate_rdma_wrapper(connection_t *conn, std::vector<std::string> &keys,
                                int block_size) {
    std::vector<remote_block_t> blocks;
    allocate_rdma(conn, keys, block_size, blocks);
    return as_pyarray(std::move(blocks));
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
    m.def("r_rdma", &r_rdma_wrapper, "Read remote memory");
    m.def("w_rdma", &w_rdma_wrapper, "Write remote memory");

    PYBIND11_NUMPY_DTYPE(remote_block_t, rkey, remote_addr);

    m.def("allocate_rdma", &allocate_rdma_wrapper, "Allocate remote memory");
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
        .def_readwrite("prealloc_size", &ServerConfig::prealloc_size)
        .def_readwrite("minimal_allocate_size", &ServerConfig::minimal_allocate_size)
        .def_readwrite("num_stream", &ServerConfig::num_stream)
        .def_readwrite("auto_increase", &ServerConfig::auto_increase);
    m.def("get_kvmap_len", &get_kvmap_len, "get kv map size");
    m.def("register_server", &register_server, "register the server");

    // //both side
    m.def("log_msg", &log_msg, "log");
    m.def("set_log_level", &set_log_level, "set log level");
}
