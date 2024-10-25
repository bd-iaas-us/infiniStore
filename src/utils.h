#ifndef UTILS_H
#define UTILS_H

#include <stddef.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include "protocol.h"
#include <boost/intrusive_ptr.hpp>
#include <atomic>



#define CHECK_CUDA(call) \
    do { \
        cudaError_t err = call; \
        if (err != cudaSuccess) { \
            fprintf(stderr, "CUDA error in %s:%d: %s\n", __FILE__, __LINE__, cudaGetErrorString(err)); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

#define CHECK_GDR(call) \
    do { \
        int ret = call; \
        if (ret) { \
            fprintf(stderr, "GDRCopy error in %s:%d: %d\n", __FILE__, __LINE__, ret); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

int send_exact(int socket, const void *buffer, size_t length);
int recv_exact(int socket, void *buffer, size_t length);
void print_ipc_handle(cudaIpcMemHandle_t ipc_handle);
void compare_ipc_handle(cudaIpcMemHandle_t ipc_handle1, cudaIpcMemHandle_t ipc_handle2);
void print_rdma_conn_info(rdma_conn_info_t *info, bool is_remote);


template <typename T>
void print_vector(T* ptr, size_t size);


class IntrusivePtrTarget {
public:
    IntrusivePtrTarget() : ref_count(0) {}

    virtual ~IntrusivePtrTarget() = default;

    IntrusivePtrTarget(const IntrusivePtrTarget&) = delete;
    IntrusivePtrTarget& operator=(const IntrusivePtrTarget&) = delete;

    friend void intrusive_ptr_add_ref(IntrusivePtrTarget* p) {
        p->ref_count.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(IntrusivePtrTarget* p) {
        if (p->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete p;
        }
    }

private:
    mutable std::atomic<int> ref_count;
};

#endif // UTILS_H
