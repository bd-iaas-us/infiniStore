cmake_minimum_required(VERSION 3.18)
project(common)

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

# fmt
find_library(FMT_LIBRARY NAMES fmt)
if (NOT FMT_LIBRARY)
    message(FATAL_ERROR "fmt not found. Make sure libfmt is installed.")
endif()

# common
include_directories(include)
add_library(common SHARED ibv_helper.cpp log.cpp protocol.cpp utils.cpp)

target_link_libraries(common PRIVATE ${FMT_LIBRARY} ${IBVERBS_LIBRARY} ${CUDART_LIBRARY})
target_include_directories(common PUBLIC ${CUDA_INCLUDE_DIRS} ${IBVERBS_INCLUDE_DIR})

# install(TARGETS common DESTINATION ${CMAKE_SOURCE_DIR}/infinistore)