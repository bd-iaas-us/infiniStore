#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include "libinfinity.h"

namespace py = pybind11;
extern int register_server(unsigned int loop_ptr);

int rw_local_wrapper(connection_t *conn, char op, const std::string &key , unsigned long ptr, unsigned long offset, size_t size) {    
    return rw_local(conn, op, key.c_str(), key.size(), (void*)ptr, offset, size);
}

PYBIND11_MODULE(_infinity, m) {
    py::class_<connection_t>(m, "Connection")
        .def(py::init<>())
        .def_readwrite("sock", &connection_t::sock);

    m.def("init_connection", &init_connection, "Initialize a connection");
    m.def("close_connection", &close_connection, "Close a connection");
    m.def("rw_local", &rw_local_wrapper, "Read/Write cpu memory from GPU device");
    m.def("register_server", &register_server, "register the server");

}


