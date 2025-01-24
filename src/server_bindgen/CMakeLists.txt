cmake_minimum_required(VERSION 3.18)
project(server_bindgen)

set(CMAKE_CXX_STANDARD 17)

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src/common/include)
include_directories(${CMAKE_SOURCE_DIR}/src/server/include)

Python_add_library(server_bindgen MODULE server_bindgen.cpp)
target_link_libraries(server_bindgen PRIVATE pybind11::headers common server)
target_include_directories(server_bindgen PUBLIC ${CMAKE_SOURCE_DIR}/src/common/include ${CMAKE_SOURCE_DIR}/src/server/include)

set_target_properties(server_bindgen PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
)
