cmake_minimum_required(VERSION 3.18)
project(client_bindgen)

set(CMAKE_CXX_STANDARD 17)

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src/common/include)
include_directories(${CMAKE_SOURCE_DIR}/src/client/include)

Python_add_library(client_bindgen MODULE client_bindgen.cpp)
target_link_libraries(client_bindgen PRIVATE pybind11::headers common client)
target_include_directories(client_bindgen PUBLIC ${CMAKE_SOURCE_DIR}/src/common/include ${CMAKE_SOURCE_DIR}/src/client/include)

set_target_properties(client_bindgen PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
)
