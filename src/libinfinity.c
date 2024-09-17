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
#include <math.h>
#include "libinfinity.h"


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

void close_connection(connection_t *conn) {
    if (conn->sock > 0) {
        close(conn->sock);
    }
}


int rw_local(connection_t *conn, char op, const void *key_ptr, size_t key_size, void *ptr, unsigned long offset, size_t size) {
    assert(conn != NULL);
    assert(ptr != NULL);
    assert(size > 0);

    cudaIpcMemHandle_t ipc_handle;
    memset(&ipc_handle, 0, sizeof(cudaIpcMemHandle_t));

     
    CHECK_CUDA(cudaIpcGetMemHandle(&ipc_handle, ptr));
    //print_ipc_handle(ipc_handle);

    header_t header;
    header = {
        .magic = MAGIC,
        .op = op,
        .ipc_handle = ipc_handle,
        .offset = offset,
        .payload_size = size,
        .key_size = key_size
    };
    
    //send header
    send_exact(conn->sock, &header, FIXED_HEADER_SIZE);

    //send key
    send_exact(conn->sock, key_ptr, key_size);

    int return_code;
    recv(conn->sock, &return_code, RETURN_CODE_SIZE, MSG_WAITALL);

    if (return_code != FINISH) {
        return -1;
    }
    return 0;
} 