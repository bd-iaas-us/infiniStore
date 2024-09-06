#include <cuda.h>
#include <cuda_runtime.h>
#include <gdrapi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "protocol.h"
#include "utils.h"
#include <assert.h>
#include <time.h>



// 定义个一个struct， 包括socket
typedef struct {
    int sock;
}connection_t;



int connect_common() {

}

int init_connection(connection_t *conn)   {
    assert(conn != NULL);
    int sock = 0;

    struct sockaddr_in serv_addr;
    // create socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // always connect to localhost
    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }

    conn->sock = sock;
    return 0;
}

int close_connection(connection_t *conn) {
    assert(conn != NULL);
    if (conn->sock > 0) {
        close(conn->sock);
    }
}

int write_local(connection_t *conn, const void *key_ptr, size_t key_size, void *ptr, size_t size) {
    assert(conn != NULL);
    assert(ptr != NULL);
    assert(size > 0);

    cudaIpcMemHandle_t ipc_handle;
    int ret;

    // send the magic number
    int magic = MAGIC;
    //send(conn->sock, &magic, MAGIC_SIZE, 0);
    send_exact(conn->sock, &magic, MAGIC_SIZE);

    // send the operation
    char op;
    op = OP_W;
    send_exact(conn->sock, &op, OP_SIZE);

    // send the size of the key
    send_exact(conn->sock, &key_size, sizeof(int));

    // send the key
    send_exact(conn->sock, key_ptr, key_size);

    CHECK_CUDA(cudaIpcGetMemHandle(&ipc_handle, ptr));
    // send the ipc handle
    send_exact(conn->sock, &ipc_handle, sizeof(cudaIpcMemHandle_t));

    // send the size of the data
    send_exact(conn->sock, &size, sizeof(size_t));

    int return_code;
    recv_exact(conn->sock, &return_code, RETURN_CODE_SIZE);
    if (return_code != FINISH) {
        return -1;
    }
    return 0;
} 

int main() {

    int size = 2024 * sizeof(float);
    void *d_ptr;
    int ret;
    // allocate cuda memory
    CHECK_CUDA(cudaMalloc(&d_ptr, size));

    // set data
    float* h_data = (float*)malloc(size);
    for (int i = 0; i < 1024; i++) {
        h_data[i] = float(i);
    }
    CHECK_CUDA(cudaMemcpy(d_ptr, h_data, size, cudaMemcpyHostToDevice));
    free(h_data);

    connection_t conn;
    ret = init_connection(&conn);
    if (ret != 0) {
        printf("Failed to init connection\n");
        return -1;
    }
    
    ret = write_local(&conn, "test", 4,  d_ptr, size);
    if (ret < 0) {
        printf("Failed to write local\n");
        CHECK_CUDA(cudaFree(d_ptr));
        close_connection(&conn);
        return -1;
    }

    CHECK_CUDA(cudaFree(d_ptr));
    printf("Write local success\n");
    close_connection(&conn);
}