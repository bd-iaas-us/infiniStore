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

JEMemoryPool::JEMemoryPool(size_t pool_size, struct ibv_pd *pd)
    : pool_(nullptr), pool_size_(pool_size), arena_ind_(0), mr_(nullptr), offset_(0) {
    memset(&custom_hooks_, 0, sizeof(custom_hooks_));

    INFO("JEMemory pool size: {} bytes", pool_size_);

    if (posix_memalign(&pool_, 4096, pool_size_) != 0) {
        ERROR("Failed to allocate jemalloc memory pool");
        exit(EXIT_FAILURE);
    }

    CHECK_CUDA(cudaHostRegister(pool_, pool_size_, cudaHostRegisterDefault));

    INFO("JEMemory pool allocated at {}", pool_);

    mr_ = ibv_reg_mr(pd, pool_, pool_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr_) {
        ERROR("Failed to register MR");
        exit(EXIT_FAILURE);
    }

    size_t sz = sizeof(arena_ind_);
    if (mallctl("arenas.create", &arena_ind_, &sz, nullptr, 0) != 0) {
        ERROR("mallctl arenas.create failed");
        exit(EXIT_FAILURE);
    }

    custom_hooks_ = {
        .hooks = {
            .alloc = [](extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment,
                        bool *zero, bool *commit, unsigned arena_ind) -> void * {
                DEBUG("custom_extent_alloc called: size: {}, alignment: {}", size, alignment);
                JEMemoryPool *pool =
                    static_cast<JEMemoryPool *>(((custom_extent_hooks_t *)extent_hooks)->user_data);
                size_t aligned_offset = (pool->offset_ + alignment - 1) & ~(alignment - 1);

                if (aligned_offset + size > pool->pool_size_) {
                    ERROR("JEMemoryPool out of memory, offset: {}, size: {}, pool_size: {}",
                          pool->offset_, size, pool->pool_size_);
                    return NULL;
                }

                void *result = (void *)((uintptr_t)pool->pool_ + aligned_offset);
                pool->offset_ = aligned_offset + size;
                if (*zero) {
                    memset(result, 0, size);
                }
                return result;
            },
            .dalloc = [](extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed,
                         unsigned arena_ind) -> bool { return false; }},
        .user_data = this,
    };

    char cmd[128];
    snprintf(cmd, sizeof(cmd), "arena.%u.extent_hooks", arena_ind_);
    extent_hooks_t *new_hooks = (extent_hooks_t *)(&custom_hooks_);
    if (mallctl(cmd, nullptr, nullptr, &new_hooks, sizeof(new_hooks)) != 0) {
        ERROR("mallctl setting extent_hooks failed");
        exit(EXIT_FAILURE);
    }
}

bool JEMemoryPool::allocate(size_t size, size_t n, SimpleAllocationCallback callback) {
    size_t required_size = size * n;
    if (offset_ + required_size > pool_size_) {
        return false;
    }

    for (size_t i = 0; i < n; ++i) {
        // minimum alignment is 32KB
        void *addr = mallocx(size, MALLOCX_ARENA(arena_ind_) | MALLOCX_ALIGN(4096));
        if (!addr) {
            return false;
        }
        callback(addr, mr_->lkey, mr_->rkey);
    }
    return true;
}

void JEMemoryPool::deallocate(void *ptr, size_t size) { dallocx(ptr, MALLOCX_ARENA(arena_ind_)); }

JEMemoryPool::~JEMemoryPool() {
    if (mr_) {
        ibv_dereg_mr(mr_);
    }
    if (pool_) {
        // cudaHostUnregister(pool_);
        free(pool_);
    }
}
