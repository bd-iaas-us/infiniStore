#ifndef LIBINFINITY_H
#define LIBINFINITY_H

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <assert.h>
#include <time.h>
#include "protocol.h"

//typedef struct connection connection_t;

typedef struct {
    int sock;
} connection_t;

int init_connection(connection_t *conn);
void close_connection(connection_t *conn);
//async rw local cpu memory, even rw_local returns, it is not guaranteed that the operation is completed until sync_local is recved.
int rw_local(connection_t *conn, char op, const void *key_ptr, size_t key_size, void *ptr, unsigned long offset, size_t size);
int sync_local(connection_t *conn);
#endif // LIBINFINITY_H