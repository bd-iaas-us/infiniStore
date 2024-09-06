#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include "libinfinity.h"

namespace py = pybind11;

PYBIND11_MODULE(_infinity, m) {
    py::class_<connection_t>(m, "Connection")
        .def(py::init<>())
        .def_readwrite("sock", &connection_t::sock);

    m.def("init_connection", &init_connection, "Initialize a connection");
    m.def("close_connection", &close_connection, "Close a connection");
    m.def("rw_local", &rw_local, "Read/Write local data",
          py::arg("conn"), py::arg("op"), py::arg("key_ptr"), py::arg("key_size"), py::arg("ptr"), py::arg("size"));
}