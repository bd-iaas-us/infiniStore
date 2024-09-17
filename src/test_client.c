#include <cuda.h>
#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "protocol.h"
#include "utils.h"
#include <assert.h>
#include <time.h>
#include <math.h>
#include "libinfinity.h"

void compare_floats(const float* h_data, const float* h_data2, size_t size, float epsilon) {
    for (size_t i = 0; i < size; i++) {
        if (fabs(h_data[i] - h_data2[i]) > epsilon) {
            printf("Data not equal at index %zu: %f vs %f\n", i, h_data[i], h_data2[i]);
            break;
        }
    }
}
int main() {

    int size = 1024 * sizeof(float);
    void *d_ptr;
    void *d_ptr2;
    float *h_data;
    float *h_data2;
    int ret;
    // allocate cuda memory
    CHECK_CUDA(cudaMalloc(&d_ptr, size));

    // set data
    h_data = (float*)malloc(size);
    for (int i = 0; i < 1024; i++) {
        h_data[i] = float(i) + 100;
    }
    CHECK_CUDA(cudaMemcpy(d_ptr, h_data, size, cudaMemcpyHostToDevice));

    connection_t conn;
    ret = init_connection(&conn);
    if (ret != 0) {
        printf("Failed to init connection\n");
        goto out;
    }
    
    ret = rw_local(&conn, OP_W, "test", 4,  d_ptr, 0, size);
    if (ret < 0) {
        printf("Failed to write local memory %d\n", ret);
        goto out;
    }
    ret = sync_local(&conn);
    if (ret < 0) {
        printf("Failed to sync local memory\n");
        goto out;
    }
    



    //allocate a new cuda memory    
    CHECK_CUDA(cudaMalloc(&d_ptr2, size));
    printf("out:print address of d_ptr:  %p\n", d_ptr);
    printf("out:print address of d_ptr2: %p\n", d_ptr2);

    ret = rw_local(&conn, OP_R, "test", 4, d_ptr2, 0, size);
    if (ret < 0) {
        goto out;
        return -1;
    }
    ret = sync_local(&conn);
    if (ret < 0) {
        goto out;
        return -1;
    }

    //compare the data in d_ptr and d_ptr2
    h_data2 = (float*)malloc(size);
    CHECK_CUDA(cudaMemcpy(h_data2, d_ptr2, size, cudaMemcpyDeviceToHost));

    //compare h_data and h_data2
    compare_floats(h_data, h_data2, 1024, 1e-5);


out:
    if (h_data2 != NULL) {
        free(h_data2);
        h_data2 = NULL;
    }
    if (h_data != NULL) {
        free(h_data);
        h_data = NULL;
    }
    if (d_ptr != NULL) {
        CHECK_CUDA(cudaFree(d_ptr));
    }
    if (d_ptr2 != NULL) {
        CHECK_CUDA(cudaFree(d_ptr2));
    }

    printf("read/write local cpu memory success\n");
    close_connection(&conn);
}