#ifndef PROTOCOL_H
#define PROTOCOL_H


#include <cuda.h>
#include <cuda_runtime.h>

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
    unsigned int meta_size;
} header_t;

typedef struct __attribute__((packed)) {
    unsigned long offset;
    char * key; //变长字符串
} block_t;


typedef struct __attribute__((packed)){
    cudaIpcMemHandle_t ipc_handle;
    int block_size;
    block_t *blocks;
} local_meta_t;

typedef struct __attribute__((packed)) {
    //
} remote_meta_t;

char *serialize_local_meta(local_meta_t *meta, size_t *out_size);
local_meta_t *deserialize_local_meta(const char *data, size_t size);
void free_local_meta(local_meta_t *meta);

#define FIXED_HEADER_SIZE sizeof(header_t)

#endif