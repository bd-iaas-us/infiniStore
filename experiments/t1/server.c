#include <cuda.h>
#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <gdrapi.h>
#include <time.h>

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
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    void* d_ptr;
    size_t size = 1024 * sizeof(int);
    cudaIpcMemHandle_t ipc_handle;

    // 分配CUDA内存
    CHECK_CUDA(cudaMalloc(&d_ptr, size));

    // 初始化数据
    int* h_data = (int*)malloc(size);
    for (int i = 0; i < 1024; i++) {
        h_data[i] = i;
    }
    CHECK_CUDA(cudaMemcpy(d_ptr, h_data, size, cudaMemcpyHostToDevice));
    free(h_data);

    // 获取IPC句柄
    CHECK_CUDA(cudaIpcGetMemHandle(&ipc_handle, d_ptr));

    // 创建socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 设置socket选项
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // 绑定socket
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 监听连接
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Server is waiting for connections...\n");

    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    // 发送IPC句柄
    send(new_socket, &ipc_handle, sizeof(cudaIpcMemHandle_t), 0);
    printf("IPC handle sent\n");
printf("ipc_handle content:\n");
for (int i = 0; i < sizeof(cudaIpcMemHandle_t); i++) {
    printf("%02x ", ((unsigned char*)&ipc_handle)[i]);
}
printf("\n");


    // 等待客户端处理完毕
    char buffer[1024] = {0};
    read(new_socket, buffer, 1024);
    printf("Client message: %s\n", buffer);

    // 清理
    CHECK_CUDA(cudaFree(d_ptr));
    close(new_socket);
    close(server_fd);

    return 0;
}
