#include "mempool.h"

#include <assert.h>
#include <sys/mman.h>

#include <cstring>
#include <iostream>
#include <stdexcept>

#include "log.h"
#include "utils.h"

MemoryPool::MemoryPool(size_t pool_size, size_t block_size, struct ibv_pd* pd)
    : pool_(nullptr), pool_size_(pool_size), block_size_(block_size), pd_(pd), mr_(nullptr) {
    // 计算总的内存块数量
    total_blocks_ = pool_size_ / block_size_;
    assert(pool_size % block_size == 0);

    INFO(
        "Memory pool size: {} bytes, block size: {} bytes, total blocks: {}, "
        "it may take a while",
        pool_size_, block_size_, total_blocks_);
    if (posix_memalign(&pool_, 4096, pool_size_) != 0) {
        ERROR("Failed to allocate memory pool");
        exit(EXIT_FAILURE);
    }

    CHECK_CUDA(cudaHostRegister(pool_, pool_size_, cudaHostRegisterDefault));

    INFO("Memory pool allocated at {}", pool_);

    // 注册内存区域
    mr_ = ibv_reg_mr(pd_, pool_, pool_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr_) {
        ERROR("Failed to register MR");
        exit(EXIT_FAILURE);
    }
    bitmap_.resize(total_blocks_, 0);
}

MemoryPool::~MemoryPool() {
    if (mr_) {
        ibv_dereg_mr(mr_);
    }
    if (pool_) {
        cudaFreeHost(pool_);
    }
}

bool MemoryPool::allocate(size_t size, size_t n, SimpleAllocationCallback callback) {
    size_t required_blocks = (size + block_size_ - 1) / block_size_;  // round up

    if (required_blocks > total_blocks_) {
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

            if (start_block + required_blocks > total_blocks_) {
                return false;
            }

            bool found = true;
            for (size_t j = 0; j < required_blocks; ++j) {
                size_t idx = (start_block + j) / bit_per_word;
                size_t bit = (start_block + j) % bit_per_word;
                if (bitmap_[idx] & (1ULL << bit)) {
                    found = false;
                    bit_index += j;  // skip all the blocks we already checked
                    break;
                }
            }

            if (found) {
                for (size_t j = 0; j < required_blocks; ++j) {
                    size_t idx = (start_block + j) / bit_per_word;
                    size_t bit = (start_block + j) % bit_per_word;
                    bitmap_[idx] |= (1ULL << bit);
                }
                void* addr = static_cast<char*>(pool_) + start_block * block_size_;
                callback(addr, mr_->lkey, mr_->rkey);
                if (++num_allocated == n) {
                    return true;
                }
            }
        }
    }

    return num_allocated == n;
}

void MemoryPool::deallocate(void* ptr, size_t size) {
    size_t blocks_to_free = size / block_size_;
    if (size % block_size_ != 0) {
        blocks_to_free += 1;  // round up
    }

    uintptr_t offset = static_cast<char*>(ptr) - static_cast<char*>(pool_);
    if (offset % block_size_ != 0) {
        std::cerr << "Invalid pointer deallocation attempt: not aligned" << std::endl;
        return;
    }

    size_t start_block = offset / block_size_;
    if (start_block >= total_blocks_) {
        ERROR("Pointer out of range");
        return;
    }

    if (start_block + blocks_to_free > total_blocks_) {
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
        auto simple_callback = [callback, i](void* ptr, uint32_t lkey, uint32_t rkey) {
            callback(ptr, lkey, rkey, i);
        };

        if (mempools_[i]->allocate(size, n, simple_callback)) {
            return true;
        }
    }
    return false;
}

void MM::deallocate(void* ptr, size_t size, int pool_idx) {
    mempools_[pool_idx]->deallocate(ptr, size);
}
