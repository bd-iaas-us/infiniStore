cmake_minimum_required(VERSION 3.18)
project(client)

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

# common
include_directories(${CMAKE_SOURCE_DIR}/src/common/include)

# client
include_directories(include)
add_library(client SHARED libinfinistore.cpp)

target_link_libraries(client PRIVATE common ${IBVERBS_LIBRARY} ${CUDART_LIBRARY})

set_target_properties(client PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
)

# install(TARGETS client DESTINATION ${CMAKE_SOURCE_DIR}/infinistore)