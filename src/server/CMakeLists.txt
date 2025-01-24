cmake_minimum_required(VERSION 3.18)
project(server)

set(CMAKE_CXX_STANDARD 17)

# CUDA
set(CMAKE_PREFIX_PATH "/usr/local/cuda")
find_package(CUDA REQUIRED)
include_directories(${CUDA_INCLUDE_DIRS})

find_library(CUDART_LIBRARY NAMES cudart)
if (NOT CUDART_LIBRARY)
    message(FATAL_ERROR "cudart not found. Make sure CUDA is installed.")
endif()

# libverbs
find_library(IBVERBS_LIBRARY NAMES ibverbs)
find_path(IBVERBS_INCLUDE_DIR NAMES infiniband/verbs.h)
if (NOT IBVERBS_LIBRARY OR NOT IBVERBS_INCLUDE_DIR)
    message(FATAL_ERROR "ibverbs not found. Make sure libibverbs-dev is installed.")
endif()

# uv
find_library(UV_LIBRARY NAMES uv)
if (NOT UV_LIBRARY)
    message(FATAL_ERROR "uv not found. Make sure libuv is installed.")
endif()

# common
include_directories(${CMAKE_SOURCE_DIR}/src/common/include)

# server
include_directories(include)
add_library(server SHARED infinistore.cpp mempool.cpp)

target_link_libraries(server PRIVATE common ${UV_LIBRARY} ${IBVERBS_LIBRARY} ${CUDART_LIBRARY})

set_target_properties(server PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
)

# install(TARGETS server DESTINATION ${CMAKE_SOURCE_DIR}/infinistore)