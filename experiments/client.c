#include <cuda.h>
#include <cuda_runtime.h>
#include <gdrapi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define PORT 8080
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

int main() {
	cudaSetDevice(0);
    int sock = 0;
    struct sockaddr_in serv_addr;
    cudaIpcMemHandle_t ipc_handle;
    void* d_ptr;
    size_t size = 1024 * sizeof(int);

    // 创建socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // 转换IPv4地址从文本到二进制形式
    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    // 连接到服务器
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }

    // 接收IPC句柄
    recv(sock, &ipc_handle, sizeof(cudaIpcMemHandle_t), 0);
    printf("IPC handle received\n");
    printf("ipc_handle content:\n");
    for (int i = 0; i < sizeof(cudaIpcMemHandle_t); i++) {
	    printf("%02x ", ((unsigned char*)&ipc_handle)[i]);
    }
    printf("\n");


    // 打开IPC内存句柄
    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    CHECK_CUDA(cudaDeviceSynchronize());


    bool gdr = true;
 // 分配 CPU 内存
    void * h_dst = malloc(size);

    if (h_dst == NULL) {
	    perror("Failed to allocat host memroy");
	    exit(EXIT_FAILURE);
    }
    #define GPU_PAGE_SHIFT 16
#define GPU_PAGE_SIZE (1UL << GPU_PAGE_SHIFT)
#define GPU_PAGE_OFFSET (GPU_PAGE_SIZE - 1)
#define GPU_PAGE_MASK (~GPU_PAGE_OFFSET)

    if (gdr) {

	    // 初始化GDRCopy
	    gdr_t g = gdr_open();
	    if (!g) {
		    fprintf(stderr, "Error opening gdr\n");
		    exit(1);
	    }

	    // 映射内存
	    gdr_mh_t mh;
	    void* map_d_ptr = NULL;
	    // Ensure pointer is page-aligned
	    //
	    if ((uintptr_t)d_ptr % sysconf(_SC_PAGESIZE) != 0) {
		    fprintf(stderr, "Pointer is not page-aligned\n");
		    return -1;
	    }

	    printf("d_ptr:%lu\n", (unsigned long)d_ptr);
	    cudaPointerAttributes attr;

	    // Check for a valid CUDA device pointer (simplified check)
	    if (cudaPointerGetAttributes(&attr, d_ptr) != cudaSuccess || attr.type != cudaMemoryTypeDevice) {
		    fprintf(stderr, "Invalid GPU pointer\n");
		    return -1;
	    }

	    CHECK_GDR(gdr_pin_buffer(g, (CUdeviceptr)d_ptr, size, 0, 0, &mh));
	    CHECK_GDR(gdr_map(g, mh, &map_d_ptr, size));

	    // 分配主机内存并使用GDRCopy进行拷贝
	    CHECK_GDR(gdr_copy_from_mapping(mh, h_dst, map_d_ptr, size));

	    // 验证数据
	    int* result = (int*)h_dst;
	    for (int i = 0; i < 10; i++) {
		    printf("%d ", result[i]);
	    }
	    printf("\n");

	    // 清理
	    CHECK_GDR(gdr_unmap(g, mh, map_d_ptr, size));
	    CHECK_GDR(gdr_unpin_buffer(g, mh));
	    gdr_close(g);
    } else {

    // 创建 CUDA 流
    cudaStream_t stream;
    CHECK_CUDA(cudaStreamCreate(&stream));

    // 使用 cudaMemcpyAsync 进行异步内存拷贝（从 GPU 到 CPU）
    CHECK_CUDA(cudaMemcpyAsync(h_dst, d_ptr, size, cudaMemcpyDeviceToHost, stream));

    // 同步流以确保拷贝完成
    CHECK_CUDA(cudaStreamSynchronize(stream));

    // 验证数据（这里只打印前10个元素）
    for (int i = 0; i < 10; i++) {
        printf("%d ", ((int*)h_dst)[i]);
    }
    printf("\n");

}
    CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));

    // 发送完成消息给服务器
    char *message = "Data received and processed";
    send(sock, message, strlen(message), 0);

    close(sock);
    return 0;
}
