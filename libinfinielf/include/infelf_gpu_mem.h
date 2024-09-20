#ifndef __INFELF_GPU_MEM_H__
#define __INFELF_GPU_MEM_H__

#include <stdlib.h>
#include "infelf.h"

int infelf_gpu_init(const char *bdf);
void infelf_gpu_close();
void *infelf_gpu_buffer_alloc(size_t length, int dev_id);
void infelf_gpu_buffer_free(void *buff);

#endif
