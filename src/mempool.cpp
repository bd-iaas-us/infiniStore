#include "mempool.h"

#include <assert.h>
#include <jemalloc/jemalloc.h>
#include <sys/mman.h>

#include <cstring>
#include <iostream>
#include <stdexcept>

#include "log.h"
#include "utils.h"

BitmapMemoryPool::BitmapMemoryPool(size_t pool_size, size_t chunk_size, struct ibv_pd *pd)
    : pool_(nullptr), pool_size_(pool_size), chunk_size_(chunk_size), mr_(nullptr) {
    total_chunks_ = pool_size_ / chunk_size_;
    assert(pool_size % chunk_size == 0);

    INFO(
        "BitmapMemory pool size: {} bytes, chunk size: {} bytes, total blocks: {}, "
        "it may take a while",
        pool_size_, chunk_size_, total_chunks_);
    if (posix_memalign(&pool_, 4096, pool_size_) != 0) {
        ERROR("Failed to allocate memory pool");
        exit(EXIT_FAILURE);
    }

    CHECK_CUDA(cudaHostRegister(pool_, pool_size_, cudaHostRegisterDefault));

    INFO("BitmapMemory pool allocated at {}", pool_);

    mr_ = ibv_reg_mr(pd, pool_, pool_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr_) {
        ERROR("Failed to register MR");
        exit(EXIT_FAILURE);
    }
    bitmap_.resize(total_chunks_, 0);
}

BitmapMemoryPool::~BitmapMemoryPool() {
    if (mr_) {
        ibv_dereg_mr(mr_);
    }
    if (pool_) {
        cudaHostUnregister(pool_);
        free(pool_);
    }
}

bool BitmapMemoryPool::allocate(size_t size, size_t n, SimpleAllocationCallback callback) {
    size_t required_chunks = (size + chunk_size_ - 1) / chunk_size_;  // round up

    if (required_chunks > total_chunks_) {
        return false;
    }

    int num_allocated = 0;
    size_t bit_per_word = 64;

    for (size_t word_index = 0; word_index < bitmap_.size(); ++word_index) {
        uint64_t word = bitmap_[word_index];
        if (word == 0xFFFFFFFFFFFFFFFFULL) {
            continue;
        }

        for (size_t bit_index = 0; bit_index < bit_per_word; ++bit_index) {
            size_t start_block = word_index * bit_per_word + bit_index;

            if (start_block + required_chunks > total_chunks_) {
                return false;
            }

            bool found = true;
            for (size_t j = 0; j < required_chunks; ++j) {
                size_t idx = (start_block + j) / bit_per_word;
                size_t bit = (start_block + j) % bit_per_word;
                if (bitmap_[idx] & (1ULL << bit)) {
                    found = false;
                    bit_index += j;  // skip all the blocks we already checked
                    break;
                }
            }

            if (found) {
                for (size_t j = 0; j < required_chunks; ++j) {
                    size_t idx = (start_block + j) / bit_per_word;
                    size_t bit = (start_block + j) % bit_per_word;
                    bitmap_[idx] |= (1ULL << bit);
                }
                void *addr = static_cast<char *>(pool_) + start_block * chunk_size_;
                callback(addr, mr_->lkey, mr_->rkey);
                if (++num_allocated == n) {
                    return true;
                }
            }
        }
    }

    return num_allocated == n;
}

void BitmapMemoryPool::deallocate(void *ptr, size_t size) {
    size_t blocks_to_free = size / chunk_size_;
    if (size % chunk_size_ != 0) {
        blocks_to_free += 1;  // round up
    }

    uintptr_t offset = static_cast<char *>(ptr) - static_cast<char *>(pool_);
    if (offset % chunk_size_ != 0) {
        std::cerr << "Invalid pointer deallocation attempt: not aligned" << std::endl;
        return;
    }

    size_t start_block = offset / chunk_size_;
    if (start_block >= total_chunks_) {
        ERROR("Pointer out of range");
        return;
    }

    if (start_block + blocks_to_free > total_chunks_) {
        ERROR("Deallocation size out of range");
        return;
    }

    for (size_t i = start_block; i < start_block + blocks_to_free; ++i) {
        size_t idx = i / 64;
        size_t bit = i % 64;
        if (bitmap_[idx] & (1ULL << bit)) {
            bitmap_[idx] &= ~(1ULL << bit);
        }
        else {
            ERROR("Double free detected at block index {}", i);
        }
    }
}

bool MM::allocate(size_t size, size_t n, AllocationCallback callback) {
    for (int i = 0; i < mempools_.size(); ++i) {
        // create a new callback from the original callback
        auto simple_callback = [callback, i](void *ptr, uint32_t lkey, uint32_t rkey) {
            callback(ptr, lkey, rkey, i);
        };

        if (mempools_[i]->allocate(size, n, simple_callback)) {
            return true;
        }
    }
    return false;
}

void MM::deallocate(void *ptr, size_t size, int pool_idx) {
    mempools_[pool_idx]->deallocate(ptr, size);
}

// I use this to pass instance JEMemoryPool
typedef struct {
    extent_hooks_t hooks;
    void *user_data;
} custom_extent_hooks_t;

typedef struct {
    void *base;     // 内存池的起始地址
    size_t size;    // 内存池的总大小
    size_t offset;  // 当前已分配的偏移量
} mempool_t;

mempool_t mempool;

void *custom_extent_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size,
                          size_t alignment, bool *zero, bool *commit, unsigned arena_ind) {
    INFO("custom_extent_alloc called");
    size_t aligned_offset = (mempool.offset + alignment - 1) & ~(alignment - 1);
    if (aligned_offset + size > mempool.size) {
        // 内存池空间不足
        return NULL;
    }
    void *result = (void *)((uintptr_t)mempool.base + aligned_offset);
    mempool.offset = aligned_offset + size;
    if (*zero) {
        memset(result, 0, size);
    }
    return result;
}

// 自定义的内存释放函数
bool custom_extent_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed,
                          unsigned arena_ind) {
    // 在这个简单的示例中，不执行实际的释放操作
    return false;  // 返回 false 表示成功
}

extent_hooks_t custom_hooks = {
    .alloc = custom_extent_alloc,
    .dalloc = custom_extent_dalloc,
};

JEMemoryPool::JEMemoryPool(size_t pool_size, struct ibv_pd *pd)
    : pool_size_(pool_size), arena_ind_(0), mr_(nullptr) {
    INFO("JEMemory pool size: {} bytes", pool_size_);
    // if (posix_memalign(&pool_, 4096, pool_size_) != 0) {
    //     ERROR("Failed to allocate jemalloc memory pool");
    //     exit(EXIT_FAILURE);
    // }

    // CHECK_CUDA(cudaHostRegister(pool_, pool_size_, cudaHostRegisterDefault));

    // INFO("JEMemory pool allocated at {}", pool_);

    // mr_ = ibv_reg_mr(pd, pool_, pool_size_,
    //                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // if (!mr_) {
    //     ERROR("Failed to register MR");
    //     exit(EXIT_FAILURE);
    // }

    if (posix_memalign(&mempool.base, 4096, pool_size) != 0) {
        perror("posix_memalign failed");
    }

    mr_ = ibv_reg_mr(pd, mempool.base, pool_size,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr_) {
        perror("ibv_reg_mr failed");
    }
    mempool.size = pool_size;
    mempool.offset = 0;

    // 创建新的 arena
    unsigned arena_ind;
    size_t sz = sizeof(arena_ind);
    if (mallctl("arenas.create", &arena_ind, &sz, NULL, 0) != 0) {
        perror("mallctl arenas.create failed");
    }

    // 设置自定义的 extent_hooks
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "arena.%u.extent_hooks", arena_ind);
    extent_hooks_t *new_hooks = &custom_hooks;
    printf("size of new_hooks: %lu, %lu, %lu\n", sizeof(new_hooks), sizeof(extent_hooks_t *),
           sizeof(extent_hooks_t));
    if (mallctl(cmd, nullptr, nullptr, &new_hooks, sizeof(new_hooks)) != 0) {
        perror("mallctl setting extent_hooks failed");
    }
    arena_ind_ = arena_ind;
}

bool JEMemoryPool::allocate(size_t size, size_t n, SimpleAllocationCallback callback) {
    size_t required_size = size * n;
    if (mempool.offset + required_size > pool_size_) {
        return false;
    }

    for (size_t i = 0; i < n; ++i) {
        INFO("Allocating {} bytes from JEMemoryPool {}", size, arena_ind_);
        void *addr = mallocx(size, MALLOCX_ARENA(arena_ind_));
        INFO("done allocating {}", (uintptr_t)addr);
        callback(addr, mr_->lkey, mr_->rkey);
    }
    return true;
}

void JEMemoryPool::deallocate(void *ptr, size_t size) { dallocx(ptr, MALLOCX_ARENA(arena_ind_)); }

JEMemoryPool::~JEMemoryPool() {
    if (mr_) {
        ibv_dereg_mr(mr_);
    }
}
