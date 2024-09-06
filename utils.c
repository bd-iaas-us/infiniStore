
#include "utils.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdio.h>



int send_exact(int socket, const void *buffer, size_t length) {
    size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t bytes_sent = send(socket, (const char *)buffer + total_sent, length - total_sent, 0);
        if (bytes_sent < 0) {
            return errno; // Return the error code if send fails
        }
        total_sent += bytes_sent;
    }
    return 0; // Successfully sent exactly `length` bytes
}

int recv_exact(int socket, void *buffer, size_t length) {
    size_t total_received = 0;
    while (total_received < length) {
        ssize_t bytes_received = recv(socket, (char *)buffer + total_received, length - total_received, 0);
        //printf("bytes_received: %d\n", bytes_received);
        if (bytes_received < 0) {
            return errno; // Return the error code if recv fails
        } else if (bytes_received == 0) {
            return ECONNRESET; // Connection reset by peer
        }
        total_received += bytes_received;
    }
    return 0; // Successfully received exactly `length` bytes
}

//print the ipc handle, for debug
void print_ipc_handle(cudaIpcMemHandle_t ipc_handle) {
    printf("ipc_handle content:\n");
    for (int i = 0; i < sizeof(cudaIpcMemHandle_t); i++) {
	    printf("%02x ", ((unsigned char*)&ipc_handle)[i]);
    }
    printf("\n");
}

//print the vector, for debug
void print_vector(void * ptr) {
    printf("vector content:\n");
    float * p = (float *)ptr;
    for (int i = 0; i < 10; i++) {
        printf("%f ", p[i]);
    }
    printf("\n");
}

