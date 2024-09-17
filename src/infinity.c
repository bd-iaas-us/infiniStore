//single thread right now.
#include <cuda.h>
#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <gdrapi.h>
#include <time.h>
#include <assert.h>

#include "protocol.h"
#include "utils.h"
#include <string>
#include <map>

#include <uv.h>



uv_loop_t *loop;
uv_tcp_t server;
#define BUFFER_SIZE 12345
std::map<std::string, void*> kv_map;


void print_header(header_t *header) {
    printf("op: %c\n", header->op);
    if (header->op == OP_SYNC) {
       return;
    }
    printf("key_size: %d\n", header->key_size);
    print_ipc_handle(header->ipc_handle);
    printf("payload_size: %d\n", header->payload_size);
}

typedef enum {
    READ_HEADER,
    READ_KEY,
    CUDA_SYNC,
} read_state_t;


typedef struct {
    uv_tcp_t* handle; //uv_stream_t
    read_state_t state; //state of the client, for parsing the request
    size_t bytes_read; //bytes read so far, for parsing the request
    size_t expected_bytes; //expected bytes to read, for parsing the request
    header_t header;
    char* key_buffer;
    char send_buffer[RETURN_CODE_SIZE]; //preallocated buffer for sending return code

    cudaStream_t cuda_stream;
    bool cuda_operation_inflight;
    //there is a thread waiting for cuda stream to complete, use this flag to avoid multiple threads waiting for the same stream
    bool cuda_sync_inflight; 
    int return_code;
    uv_work_t work_req;
} client_t;


void on_close(uv_handle_t* handle) {
    printf("free client...\n");
    client_t *client = (client_t *) handle->data;
    if(client->key_buffer == NULL) {
        free(client->key_buffer);
        client->key_buffer = NULL;
    }
    if (client->handle) {
        free(client->handle);
        client->handle = NULL;
    }
}


void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = (char *) malloc(suggested_size);
    buf->len = suggested_size;
}

int veryfy_header(header_t *header) {
    if (header->magic != MAGIC) {
        return INVALID_REQ;
    }
    if (header->op != OP_R && header->op != OP_W) {
        return INVALID_REQ;
    }
    if (header->key_size <= 0) {
        return INVALID_REQ;
    }
    if (header->payload_size <= 0) {
        return INVALID_REQ;
    }
    return 0;
}


void on_write(uv_write_t* req, int status) {
    if (status < 0) {
        fprintf(stderr, "Write error %s\n", uv_strerror(status));
        uv_close((uv_handle_t*)req->handle, on_close);
    }
    free(req);
}



int do_read_kvcache(client_t *client) {
    header_t *header = &client->header;
    assert(header != NULL);
    //get the key
    void * d_ptr;
    std::string k(client->key_buffer, header->key_size);
    //find the key in the map
    if (kv_map.find(k) == kv_map.end()) {
        //key not found
        printf("Key not found, return code: %d\n", client->key_buffer);
        return KEY_NOT_FOUND;
    }

    //key found
    void * h_src = kv_map[k];

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, header->ipc_handle, cudaIpcMemLazyEnablePeerAccess));

    cudaStream_t cuda_stream;
    CHECK_CUDA(cudaStreamCreate(&cuda_stream));

    //push the host cpu data to local device
    CHECK_CUDA(cudaMemcpyAsync(d_ptr + header->offset, h_src, header->payload_size, cudaMemcpyHostToDevice, cuda_stream));
    
    // print_vector(h_src);
    
    // CHECK_CUDA(cudaStreamSynchronize(cuda_stream));
    // CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
    client->cuda_operation_inflight = true;
    client->return_code = FINISH;
    
    return TASK_ACCEPTED;
}


int do_write_kvcache(client_t *client) {

    header_t *header = &client->header;
    assert(header != NULL);
    // allocate host memory
    void* d_ptr;

    CHECK_CUDA(cudaIpcOpenMemHandle(&d_ptr, header->ipc_handle, cudaIpcMemLazyEnablePeerAccess));
    
    //TODO: do we need to synchronize here?
    CHECK_CUDA(cudaDeviceSynchronize());
    void * h_dst = malloc(header->payload_size);
    if (h_dst == NULL) {
        perror("Failed to allocat host memroy");
        CHECK_CUDA(cudaIpcCloseMemHandle(d_ptr));
        return SYSTEM_ERROR;
    }
    //create cuda stream(async for future)
    cudaStream_t cuda_stream;
    CHECK_CUDA(cudaStreamCreate(&cuda_stream));

    //how to deal with memory overflow? 
    //pull data from local device to CPU host
    CHECK_CUDA(cudaMemcpyAsync(h_dst, d_ptr + header->offset, header->payload_size, cudaMemcpyDeviceToHost, cuda_stream));


    client->cuda_operation_inflight = true;
    client->return_code = FINISH;
    client->cuda_stream = cuda_stream;

    //cudaStreamSynchronize(cuda_stream);
    //print_vector(h_dst);
    std::string k(client->key_buffer, header->key_size);
    kv_map[k] = h_dst;

    return TASK_ACCEPTED;
}


//danger zone
void wait_for_cuda_completion(uv_work_t *req) {
    client_t *client = (client_t *)req->data;

    // Wait for the CUDA stream to complete
    cudaError_t err = cudaStreamSynchronize(client->cuda_stream);
    if (err != cudaSuccess) {
        client->return_code = SYSTEM_ERROR;
    } else {
        client->return_code = FINISH;
    }
}

void after_cuda_completion(uv_work_t *req, int status) {
    client_t *client = (client_t *)req->data;

    // Send the response to the client
    uv_write_t* write_req = (uv_write_t*)malloc(sizeof(uv_write_t));
    memcpy(client->send_buffer, &client->return_code, RETURN_CODE_SIZE);
    write_req->data = client;
    uv_buf_t wbuf = uv_buf_init(client->send_buffer, RETURN_CODE_SIZE);
    uv_write(write_req, (uv_stream_t *)client->handle, &wbuf, 1, on_write);

    // Reset client state
    client->cuda_operation_inflight = false;
    client->state = READ_HEADER;
    client->bytes_read = 0;
    client->expected_bytes = 0;
}


int do_sync_stream(client_t *client) {
    assert(client != NULL);
    if (client->cuda_operation_inflight) {
        client->work_req.data = client;
        client->state = CUDA_SYNC;
        //from google: cudaSyncStream is thread-safe.
        assert(loop != NULL);
        uv_queue_work(loop, &client->work_req, wait_for_cuda_completion, after_cuda_completion);
    } else {
        return FINISH;
    }
}

//return value of handle_request:
//if ret is less than 0, it is an system error, outer code will close the connection
//if ret is greater than 0, it is an application error or success
int handle_request(client_t *client) {
    int return_code;

    if (client->header.op == OP_W) {
        return_code = do_write_kvcache(client);
    } else if (client->header.op == OP_R) {
        return_code = do_read_kvcache(client);
    } else if (client->header.op == OP_SYNC) {
        return do_sync_stream(client);
    }

    printf("return code: %d\n", return_code);
    //if application error or success, send the return code
    uv_write_t* write_req = (uv_write_t*)malloc(sizeof(uv_write_t));
    memcpy(client->send_buffer, &return_code, RETURN_CODE_SIZE);
    write_req->data = client;
    uv_buf_t wbuf = uv_buf_init(client->send_buffer, RETURN_CODE_SIZE);
    uv_write(write_req, (uv_stream_t *)client->handle, &wbuf, 1, on_write);


    //keep connection alive
    client->state = READ_HEADER;
    client->bytes_read = 0;
    client->expected_bytes = 0;

    return 0;
}


void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    client_t* client = (client_t*)stream->data;

    //corner case: if there is a cuda operation inflight, we need to send a retry code to the client
    if (client->state == CUDA_SYNC) {
        int ret = RETRY;
        uv_write_t* write_req = (uv_write_t*)malloc(sizeof(uv_write_t));
        memcpy(client->send_buffer, &ret, RETURN_CODE_SIZE);
        write_req->data = client;
        uv_buf_t wbuf = uv_buf_init(client->send_buffer, RETURN_CODE_SIZE);
        uv_write(write_req, stream, &wbuf, 1, on_write);
        return;
    }

    if (nread < 0) {
        if (nread != UV_EOF)
            fprintf(stderr, "Read error %s\n", uv_err_name(nread));
        uv_close((uv_handle_t*)stream, on_close);
        free(buf->base);
        return;
    }

    size_t offset = 0;
    while (offset < nread) {
        switch (client->state) {
            case READ_HEADER: {
                size_t to_copy = MIN(nread - offset, FIXED_HEADER_SIZE - client->bytes_read);
                memcpy(((char*)&client->header) + client->bytes_read, buf->base + offset, to_copy);
                client->bytes_read += to_copy;
                offset += to_copy;
                if (client->bytes_read == FIXED_HEADER_SIZE) {
                    // 已经读取了固定头部，解析 key_size
                    print_header(&client->header);

                    if (client->header.op == OP_R || client->header.op == OP_W) {
                        // 读取 key
                        int ret = veryfy_header(&client->header);
                        if (ret != 0) {
                            //TODO: 返回错误码，关闭连接
                            fprintf(stderr, "Invalid header\n");
                            uv_close((uv_handle_t*)stream, on_close);
                            free(buf->base);
                            return;
                        }
                        client->expected_bytes = client->header.key_size;
                        client->bytes_read = 0; // 重置已读取字节数
                        client->key_buffer = (char*)malloc(client->expected_bytes);
                        client->state = READ_KEY;
                    } else if (client->header.op == OP_SYNC){
                        int ret = handle_request(client);
                        if (ret < 0 ) {
                            fprintf(stderr, "Write error: %s\n", uv_strerror(ret));
                            uv_close((uv_handle_t*)stream, on_close);
                            free(buf->base);
                            return;
                        }
                    }
                }
                break;
            }
            case READ_KEY: {
                if (client->key_buffer == NULL) {
                    perror("BUGON: client->buffer is NULL in READ_KEY state\n");
                    uv_close((uv_handle_t*)stream, on_close);
                    free(buf->base);
                    return;
                }
                size_t to_copy = MIN(nread - offset, client->expected_bytes - client->bytes_read);
                memcpy(client->key_buffer + client->bytes_read, buf->base + offset, to_copy);
                client->bytes_read += to_copy;
                offset += to_copy;
                if (client->bytes_read == client->expected_bytes) {
                    //do kv cache operations
                    printf("Reading key.. %s\n", client->key_buffer);

                    int ret = handle_request(client);
                    //if system error, close the connection
                    if (ret < 0 ) {
                        fprintf(stderr, "Write error: %s\n", uv_strerror(ret));
                        uv_close((uv_handle_t*)stream, on_close);
                        free(buf->base);
                        return;
                    }

                }
                break;
            }
            default:
                fprintf(stderr, "Unknown state\n");
                uv_close((uv_handle_t*)stream, on_close);
                free(buf->base);
                return;
        }
    }

    free(buf->base);
}

void on_new_connection(uv_stream_t* server, int status) {
    printf("New connection~\n");
    if (status < 0) {
        fprintf(stderr, "New connection error %s\n", uv_strerror(status));
        return;
    }
    uv_tcp_t* client_handle = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(loop, client_handle);
    if (uv_accept(server, (uv_stream_t*)client_handle) == 0) {
        client_t* client = (client_t*)malloc(sizeof(client_t));
        client->handle = client_handle;
        client_handle->data = client;
        client->state = READ_HEADER;
        client->bytes_read = 0;
        client->expected_bytes = 0;
        client->key_buffer = NULL;
        uv_read_start((uv_stream_t*)client_handle, alloc_buffer, on_read);
    } else {
        uv_close((uv_handle_t*)client_handle, NULL);
    }
}

int register_server(unsigned int loop_ptr) {
    loop = (uv_loop_t *)loop_ptr;
    assert(loop != NULL);
    uv_tcp_init(loop, &server);
    struct sockaddr_in addr;
    uv_ip4_addr("0.0.0.0", PORT, &addr);

    uv_tcp_bind(&server, (const struct sockaddr*)&addr, 0);
    int r = uv_listen((uv_stream_t*) &server, 128, on_new_connection);
    if (r) {
        fprintf(stderr, "Listen error: %s\n", uv_strerror(r));
        return 1;
    }

    return 0;
}