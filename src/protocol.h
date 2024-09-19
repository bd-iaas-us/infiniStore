#ifndef PROTOCOL_H
#define PROTOCOL_H


#include <cuda.h>
#include <cuda_runtime.h>
#include <vector>
#include <string>
#include <msgpack.hpp>

/*

Error code:
+--------------------+
| ERROR_CODE(4 bytes)|
+--------------------+

*/
#define PORT 12345

#define MAGIC 0xdeadbeef
#define MAGIC_SIZE 4

#define OP_R 'R'
#define OP_W 'W'
#define OP_SYNC 'S'
#define OP_SIZE 1

//error code: int
#define INVALID_REQ 400
#define FINISH 200
#define TASK_ACCEPTED 202
#define INTERNAL_ERROR 500
#define KEY_NOT_FOUND 404
#define RETRY 408
#define SYSTEM_ERROR 503


#define RETURN_CODE_SIZE sizeof(int)

typedef struct __attribute__((packed)){
    unsigned int magic;
    char op;
    unsigned int body_size;
} header_t;

typedef struct {
    unsigned long offset;
    std::string key;
    MSGPACK_DEFINE(offset, key)
} block_t;

typedef struct {
    cudaIpcMemHandle_t ipc_handle;
    int block_size;
    std::vector<block_t> blocks;
} local_meta_t;


// Update function declarations
bool serialize_local_meta(const local_meta_t& meta, std::string& out);
bool deserialize_local_meta(const char* data, size_t size, local_meta_t& out);

#define FIXED_HEADER_SIZE sizeof(header_t)

#endif