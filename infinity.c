//single thread right now.
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
#include <assert.h>

#include "protocol.h"
#include "utils.h"
#include <string>
#include <map>




typedef struct Header {
    char op;
    int key_size;
    char* key;
    cudaIpcMemHandle_t ipc_handle;
    int payload_size;
} header_t;

void print_header(header_t *header) {
    printf("op: %c\n", header->op);
    printf("key_size: %d\n", header->key_size);
    printf("key: %s\n", header->key);
    print_ipc_handle(header->ipc_handle);
    printf("payload_size: %d\n", header->payload_size);
}
//if ret is less than 0, it is an system error
//if ret is greater than 0, it is an application error
int recv_header(header_t *header, int socket) {

        assert(header != NULL);
        int ret;

        //read the magic number
        printf("Waiting for magic number...\n");
        int magic;
        ret = recv_exact(socket, &magic, MAGIC_SIZE);
        if (ret) {
            printf("Failed to read magic number: %s\n", strerror(ret));
            return -ret;
        }

        //only little endian is supported, check magic number
        if (magic != MAGIC) {
            printf("magic number is not correct\n");
            return INVALID_REQ;
        }

        //read 1 byte to know whether is R or W
        char op;
        ret = recv_exact(socket, &op, OP_SIZE);
        if (ret) {
            printf("Failed to read op: %d\n", ret);
            return -ret;
        }
        //check op
        if (op != OP_R && op != OP_W) {
            printf("op is not correct\n");
            return INVALID_REQ;
        }

        //read the size of the key
        int key_size;
        ret = recv_exact(socket, &key_size, sizeof(int));
        if (ret) {
            printf("Failed to read key size: %d\n", strerror(ret));
            return -ret;
        }
        if (key_size <= 0) {
            return INVALID_REQ;
        }

        //read the key
        char* key = (char*)malloc(key_size);
        if (key == NULL) {
            printf("Failed to allocate memory");
            return INTERNAL_ERROR;
        }

        ret = recv_exact(socket, key, key_size);
        if (ret) {
            printf("Failed to read key: %d\n", ret);
            free(key);
            key = NULL;
            return -ret;
        }
        printf("key: %s\n", key);

        cudaIpcMemHandle_t ipc_handle;
        //read ipc handle
        ret = recv_exact(socket, &ipc_handle, sizeof(cudaIpcMemHandle_t));
        if (ret) {
            printf("Failed to read ipc handle: %d\n", ret);
            free(key);
            key = NULL;
            return -ret;
        }
    
        printf("IPC handle received\n");
        print_ipc_handle(ipc_handle);


        //read payload size
        int payload_size;
        ret = recv_exact(socket, &payload_size, sizeof(size_t));
        if (ret) {
            free(key);
            key = NULL;
            return -ret;
        }
        if (payload_size <= 0) {
            free(key);
            key = NULL;
            return INVALID_REQ;
        }

        header->op = op;
        header->key_size = key_size;
        header->key = key;
        header->ipc_handle = ipc_handle;
        header->payload_size = payload_size;

        return 0;
}


void do_read(header_t * header, int socket, std::map<std::string, void*> &kv_map) {
    assert(header != NULL);
    //get the key
    std::string k(header->key, header->key_size);
    //find the key in the map
    if (kv_map.find(k) == kv_map.end()) {
        //key not found
        int return_code = KEY_NOT_FOUND;
        send_exact(socket, &return_code, RETURN_CODE_SIZE);
        printf("Key not found, return code:%d\n", return_code);
        return;
    }

    //key found
    void * h_dst = kv_map[k];

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, header->ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    //TODO: do we need to synchronize here?
    CHECK_CUDA(cudaDeviceSynchronize());

    cudaStream_t stream;
    CHECK_CUDA(cudaStreamCreate(&stream));

    CHECK_CUDA(cudaMemcpyAsync(d_ptr, h_dst, header->payload_size, cudaMemcpyDeviceToHost, stream));

    CHECK_CUDA(cudaStreamSynchronize(stream));

    CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));

    int return_code = FINISH;
    send_exact(socket, &return_code, RETURN_CODE_SIZE);
    printf("Finish sent, keep the socket open, return code:%d\n", return_code);
}


void do_write(header_t * header, int socket, std::map<std::string, void*> &kv_map) {

    assert(header != NULL);
    // allocate host memory
    void* d_ptr;

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, header->ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    
    //TODO: do we need to synchronize here?
    CHECK_CUDA(cudaDeviceSynchronize());
    void * h_dst = malloc(header->payload_size);
    if (h_dst == NULL) {
        perror("Failed to allocat host memroy");
        exit(EXIT_FAILURE);
    }
    //create cuda stream(async for future)
    cudaStream_t stream;
    CHECK_CUDA(cudaStreamCreate(&stream));

    CHECK_CUDA(cudaMemcpyAsync(h_dst, d_ptr, header->payload_size, cudaMemcpyDeviceToHost, stream));

    CHECK_CUDA(cudaStreamSynchronize(stream));

    CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
    
    print_vector(h_dst);
    //send finish
    std::string k(header->key, header->key_size);

    kv_map[k] = h_dst;//TODO: MAYBE WE NEED TO store the payload size as well

    int return_code = FINISH;
    send_exact(socket, &return_code, RETURN_CODE_SIZE);
    printf("Finish sent, keep the socket open, return code:%d\n", return_code);
}

int main() {
	cudaSetDevice(0);
    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);
    size_t size = 1024 * sizeof(int);
    cudaIpcMemHandle_t ipc_handle;
    unsigned int return_code;
    int ret;

    // 创建socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &reuse, sizeof(reuse))) {
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

    //use a map to store the key-value pair
    //key is a string, value is h_dst.
    std::map<std::string, void*> kv_map;

    while (true) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }
        while (true) { //keep the socket open, use pthread to handle multiple connections later
            printf("New connection~\n");
            header_t header;
            ret = recv_header(&header, new_socket);
            if (ret != 0) {
                printf("Failed to receive header\n");
                break;
            }

            print_header(&header);

            if (header.op == OP_W) {
                do_write(&header, new_socket, kv_map);
            } else if (header.op == OP_R) {
                do_read(&header, new_socket, kv_map);
            } else {
                perror("Invalid op");
            }

            if (header.key != NULL) {
                free(header.key);
                header.key = NULL;
            }
        }
    }
    //clean up
    close(server_fd);
    return 0;
}
