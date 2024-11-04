
#include "utils.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <iomanip>
#include <iostream>
#include <sstream>

#include "log.h"

int send_exact(int socket, const void* buffer, size_t length) {
    size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t bytes_sent = send(socket, (const char*)buffer + total_sent, length - total_sent, 0);
        if (bytes_sent < 0) {
            return errno;  // Return the error code if send fails
        }
        total_sent += bytes_sent;
    }
    return 0;  // Successfully sent exactly `length` bytes
}

int recv_exact(int socket, void* buffer, size_t length) {
    size_t total_received = 0;
    while (total_received < length) {
        ssize_t bytes_received =
            recv(socket, (char*)buffer + total_received, length - total_received, 0);
        // printf("bytes_received: %d\n", bytes_received);
        if (bytes_received < 0) {
            return errno;  // Return the error code if recv fails
        }
        else if (bytes_received == 0) {
            return ECONNRESET;  // Connection reset by peer
        }
        total_received += bytes_received;
    }
    return 0;  // Successfully received exactly `length` bytes
}

std::string human_readable_gid(rdma_conn_info_t* info) {
    std::string gid_str;
    bool is_ipv4_mapped = true;

    // Check if the GID is an IPv4-mapped IPv6 address
    for (int i = 0; i < 10; ++i) {
        if (info->gid.raw[i] != 0) {
            is_ipv4_mapped = false;
            break;
        }
    }
    if (info->gid.raw[10] != 0xff || info->gid.raw[11] != 0xff) {
        is_ipv4_mapped = false;
    }

    if (is_ipv4_mapped) {
        // Convert the last 4 bytes to an IPv4 address
        char ipv4_str[INET_ADDRSTRLEN];
        uint8_t ipv4_addr[4] = {info->gid.raw[12], info->gid.raw[13], info->gid.raw[14],
                                info->gid.raw[15]};
        inet_ntop(AF_INET, ipv4_addr, ipv4_str, INET_ADDRSTRLEN);
        gid_str = ipv4_str;
    }
    else {
        // Convert the GID to a standard IPv6 address string
        for (int i = 0; i < 16; ++i) {
            gid_str += fmt::format("{:02x}", info->gid.raw[i]);
            if (i % 2 == 1 && i != 15) {
                gid_str += ":";
            }
        }
    }
    return gid_str;
}

void print_rdma_conn_info(rdma_conn_info_t* info, bool is_remote) {
    std::string gid_str = human_readable_gid(info);
    if (is_remote) {
        DEBUG("remote rdma_conn_info: psn: {}, qpn: {}, gid: {}", (uint32_t)info->psn,
              (uint32_t)info->qpn, gid_str);
    }
    else {
        DEBUG("local rdma_conn_info: psn: {}, qpn: {}, gid: {}", (uint32_t)info->psn,
              (uint32_t)info->qpn, gid_str);
    }
}

void print_ipc_handle(cudaIpcMemHandle_t ipc_handle) {
    std::ostringstream oss;
    for (int i = 0; i < sizeof(cudaIpcMemHandle_t); i++) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << (int)((unsigned char*)&ipc_handle)[i] << " ";
    }
    DEBUG("ipc_handle content: {}", oss.str().c_str());
}

void compare_ipc_handle(cudaIpcMemHandle_t ipc_handle1, cudaIpcMemHandle_t ipc_handle2) {
    for (int i = 0; i < sizeof(cudaIpcMemHandle_t); i++) {
        unsigned char d1 = ((unsigned char*)&ipc_handle1)[i];
        unsigned char d2 = ((unsigned char*)&ipc_handle2)[i];
        if (d1 != d2) {
            INFO("ipc_handle1 is not equal to ipc_handle2\n");
            return;
        }
    }
}

template <typename T>
void print_vector(T* ptr, size_t size) {
    std::ostringstream oss;
    for (size_t i = 0; i < size; ++i) {
        oss << ptr[i] << " ";
    }
    DEBUG("vector content: {}", oss.str().c_str());
}

template void print_vector<float>(float* ptr, size_t size);
template void print_vector<double>(double* ptr, size_t size);
template void print_vector<int>(int* ptr, size_t size);
template void print_vector<char>(char* ptr, size_t size);
