#include "protocol.h"


#include "mpack/mpack.h"
#include <stdlib.h>
#include <string.h>

char *serialize_local_meta(local_meta_t *meta, size_t *out_size) {
    // 初始化一个可增长的 writer
    mpack_writer_t writer;
    char *data = NULL;
    size_t size = 0;

    mpack_writer_init_growable(&writer, &data, &size);

    // 开始写入数组，包含 3 个元素
    mpack_start_array(&writer, 3);

    //ipc_handle
    mpack_write_bin(&writer, (const char *)&meta->ipc_handle, sizeof(cudaIpcMemHandle_t));

    //block_size
    mpack_write_int(&writer, meta->block_size);

    //blocks array
    {
        mpack_start_array(&writer, meta->block_size);
        for (int i = 0; i < meta->block_size; ++i) {
            block_t *block = &meta->blocks[i];
            mpack_start_array(&writer, 2);
            // offset
            mpack_write_u64(&writer, block->offset);
            // key
            mpack_write_cstr(&writer, block->key);
            mpack_finish_array(&writer); //single block array done
        }
        mpack_finish_array(&writer); // all blocks array done
    }

    mpack_finish_array(&writer); // done

    // 检查错误
    if (mpack_writer_destroy(&writer) != mpack_ok) {
        free(data);
        return NULL;
    }

    *out_size = size;
    return data;
}

local_meta_t *deserialize_local_meta(const char *data, size_t size) {
    mpack_reader_t reader;
    mpack_reader_init_data(&reader, data, size);

    local_meta_t *meta = (local_meta_t *) malloc(sizeof(local_meta_t));
    memset(meta, 0, sizeof(local_meta_t));

    // 3 elements
    uint32_t array_size = mpack_expect_array(&reader);
    if (array_size != 3) {
        mpack_reader_flag_error(&reader, mpack_error_type);
        free(meta);
        return NULL;
    }

    // ipc_handle
    uint32_t ipc_handle_size = mpack_expect_bin(&reader);
    if (ipc_handle_size != sizeof(cudaIpcMemHandle_t)) {
        mpack_reader_flag_error(&reader, mpack_error_type);
        free(meta);
        return NULL;
    }
    const char *ipc_handle_data = mpack_read_bytes_inplace(&reader, ipc_handle_size);
    memcpy(&meta->ipc_handle, ipc_handle_data, sizeof(cudaIpcMemHandle_t));
    mpack_done_bin(&reader);

    //block_size
    meta->block_size = mpack_expect_int(&reader);

    //locks array
    uint32_t blocks_array_size = mpack_expect_array(&reader);
    if (blocks_array_size != (uint32_t)meta->block_size) {
        mpack_reader_flag_error(&reader, mpack_error_type);
        free(meta);
        return NULL;
    }
    meta->blocks = (block_t*)malloc(sizeof(block_t) * meta->block_size);
    memset(meta->blocks, 0, sizeof(block_t) * meta->block_size);

    for (int i = 0; i < meta->block_size; ++i) {
        block_t *block = &meta->blocks[i];

        // each block is an array of 2 elements
        uint32_t block_array_size = mpack_expect_array(&reader);
        if (block_array_size != 2) {
            mpack_reader_flag_error(&reader, mpack_error_type);
            for (int j = 0; j <= i; ++j) {
                free(meta->blocks[j].key);
            }
            free(meta->blocks);
            free(meta);
            return NULL;
        }
        //offset
        block->offset = mpack_expect_u64(&reader);
        //key
        block->key = mpack_expect_cstr_alloc(&reader, UINT32_MAX);
    }
    // check for errors
    if (mpack_reader_destroy(&reader) != mpack_ok) {
        free_local_meta(meta);
        return NULL;
    }

    return meta;
}

void free_local_meta(local_meta_t *meta) {
    if (!meta) return;
    if (meta->blocks) {
        for (int i = 0; i < meta->block_size; ++i) {
            free(meta->blocks[i].key);
        }
        free(meta->blocks);
    }
    free(meta);
}
