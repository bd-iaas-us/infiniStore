#include <pybind11/functional.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "config.h"
#include "infinistore.h"
#include "libinfinistore.h"
#include "log.h"

namespace py = pybind11;

extern int register_server(unsigned long loop_ptr, server_config_t config);

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

    PYBIND11_NUMPY_DTYPE(remote_block_t, rkey, remote_addr);

    py::class_<Connection, std::shared_ptr<Connection>>(m, "Connection")
        .def(py::init<>())
        .def("init_connection", &Connection::init_connection, "Initialize a connection")
        .def(
            "rw_local",
            [](Connection &self, char op,
               const std::vector<std::tuple<std::string, unsigned long>> &blocks, int block_size,
               uintptr_t ptr, int device_id) {
                std::vector<block_t> c_blocks;
                for (const auto &block : blocks) {
                    c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
                }
                return self.rw_local(op, c_blocks, block_size, (void *)ptr, device_id);
            },
            "Read/Write cpu memory from GPU device")

        .def(
            "r_rdma",
            [](Connection &self, const std::vector<std::tuple<std::string, unsigned long>> &blocks,
               int block_size, uintptr_t ptr) {
                std::vector<block_t> c_blocks;
                for (const auto &block : blocks) {
                    c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
                }
                return self.r_rdma(c_blocks, block_size, (void *)ptr);
            },
            "Read remote memory")

        .def(
            "w_rdma",
            [](Connection &self,
               py::array_t<unsigned long, py::array::c_style | py::array::forcecast> offsets,
               int block_size,
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
                return self.w_rdma(p_offsets, offsets_len, block_size, p_remote_blocks,
                                   remote_blocks_len, (void *)base_ptr);
            },
            "Write remote memory")

        .def(
            "r_rdma_async",
            [](Connection &self, const std::vector<std::tuple<std::string, unsigned long>> &blocks,
               int block_size, uintptr_t ptr, std::function<void()> callback) {
                std::vector<block_t> c_blocks;
                for (const auto &block : blocks) {
                    c_blocks.push_back(block_t{std::get<0>(block), std::get<1>(block)});
                }
                return self.r_rdma_async(c_blocks, block_size, (void *)ptr, [callback]() {
                    py::gil_scoped_acquire acquire;
                    callback();
                });
            },
            "Read remote memory asynchronously")

        .def(
            "w_rdma_async",
            [](Connection &self,
               py::array_t<unsigned long, py::array::c_style | py::array::forcecast> offsets,
               int block_size,
               py::array_t<remote_block_t, py::array::c_style | py::array::forcecast> remote_blocks,
               uintptr_t base_ptr, std::function<void()> callback) {
                py::buffer_info block_buf = remote_blocks.request();
                py::buffer_info offset_buf = offsets.request();

                assert(block_buf.ndim == 1);
                assert(offset_buf.ndim == 1);

                remote_block_t *p_remote_blocks = static_cast<remote_block_t *>(block_buf.ptr);
                unsigned long *p_offsets = static_cast<unsigned long *>(offset_buf.ptr);
                size_t remote_blocks_len = block_buf.shape[0];
                size_t offsets_len = offset_buf.shape[0];
                return self.w_rdma_async(p_offsets, offsets_len, block_size, p_remote_blocks,
                                         remote_blocks_len, (void *)base_ptr, [callback]() {
                                             py::gil_scoped_acquire acquire;
                                             callback();
                                         });
            },
            "Write remote memory asynchronously")

        .def(
            "allocate_rdma",
            [](Connection &self, std::vector<std::string> &keys, int block_size) {
                std::vector<remote_block_t> *blocks = self.allocate_rdma(keys, block_size);
                return as_pyarray(std::move(*blocks));
            },
            "Allocate remote memory")

        .def(
            "allocate_rdma_async",
            [](Connection &self, std::vector<std::string> &keys, int block_size,
               std::function<void(py::array)> callback) {
                self.allocate_rdma_async(keys, block_size,
                                         [callback](std::vector<remote_block_t> *blocks) {
                                             py::gil_scoped_acquire acquire;
                                             callback(as_pyarray(std::move(*blocks)));
                                             delete blocks;
                                         });
                return;
            },
            "Allocate remote memory asynchronously")

        .def("sync_local", &Connection::sync_local, "sync the cuda stream")
        .def("setup_rdma", &Connection::setup_rdma, "setup rdma connection")
        .def("sync_rdma", &Connection::sync_rdma, "sync the remote server")
        .def("check_exist", &Connection::check_exist, "check if the key exists in the store")
        .def("get_match_last_index", &Connection::get_match_last_index,
             "get the last index of a key list which is in the store")

        .def(
            "register_mr",
            [](Connection &self, uintptr_t ptr, size_t ptr_region_size) {
                return self.register_mr((void *)ptr, ptr_region_size);
            },
            "register memory region");

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
    m.def(
        "purge_kv_map", []() { kv_map.clear(); }, "purge kv map");
    m.def(
        "get_kvmap_len", []() { return kv_map.size(); }, "get kv map size");
    m.def("register_server", &register_server, "register the server");

    // //both side
    m.def("log_msg", &log_msg, "log");
    m.def("set_log_level", &set_log_level, "set log level");
}
