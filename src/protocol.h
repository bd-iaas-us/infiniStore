#ifndef PROTOCOL_H
#define PROTOCOL_H



/*
Client only send this header to the server
HEADER FORMAT(little endian):

+-------------------+--------------------------+-------------------+-------------------+
|  MAGIC (4 bytes)  |  OP (1 byte)             | KEY_SIZE (4 bytes)|  KEY              |  
+-------------------+--------------------------+-------------------+-------------------+
|  cuda ipc handler | payload size(8byte)      |  offset(8bytes)   | 
+-------------------+--------------------------+-------------------+


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

#include <cuda.h>
#include <cuda_runtime.h>

//pack the header
typedef struct __attribute__((packed)) Header {
    unsigned int magic;
    char op;
    cudaIpcMemHandle_t ipc_handle; 
    unsigned long offset;
    int payload_size;
    int key_size;
} header_t;


#define FIXED_HEADER_SIZE sizeof(header_t)

#endif