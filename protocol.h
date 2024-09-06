#ifndef PROTOCOL_H
#define PROTOCOL_H



/*
Client only send this header to the server
HEADER FORMAT(little endian):

+-------------------+-------------------+-------------------+-------------------+
|  MAGIC (4 bytes)  |  OP (1 byte)      | KEY_SIZE (4 bytes)|  KEY              |  
+-------------------+-------------------+-------------------+-------------------+
|  cuda ipc handler | payload size      | 
+-------------------+-------------------+


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
#define OP_SIZE 1

//error code: int
#define INVALID_REQ 400
#define FINISH 200
#define INTERNAL_ERROR 500
#define KEY_NOT_FOUND 404

#define RETURN_CODE_SIZE sizeof(int)


#endif