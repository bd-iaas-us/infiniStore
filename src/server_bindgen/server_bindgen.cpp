#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <config.h>
#include <log.h>
#include <infinistore.h>

namespace py = pybind11;

PYBIND11_MODULE(server_bindgen, m) {
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
        .def_readwrite("num_stream", &ServerConfig::num_stream);
    m.def("get_kvmap_len", &get_kvmap_len, "get kv map size");
    m.def("register_server", &register_server, "register the server");

    // //both side
    m.def("log_msg", &log_msg, "log");
    m.def("set_log_level", &set_log_level, "set log level");
}
