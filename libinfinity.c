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
    assert(conn != NULL);
    if (conn->sock > 0) {
        close(conn->sock);
    }
}


int rw_local(connection_t *conn, char op, const void *key_ptr, size_t key_size, void *ptr, size_t size) {
    assert(conn != NULL);
    assert(ptr != NULL);
    assert(size > 0);

    cudaIpcMemHandle_t ipc_handle;

    // send the magic number
    int magic = MAGIC;
    //send(conn->sock, &magic, MAGIC_SIZE, 0);
    send_exact(conn->sock, &magic, MAGIC_SIZE);

    // send the operation

    if (op != OP_R && op != OP_W) {
        printf("op is not correct\n");
        return -1;
    }

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