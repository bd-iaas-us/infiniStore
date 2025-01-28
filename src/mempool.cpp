#include "mempool.h"

#include <assert.h>
#include <sys/mman.h>

#include <cstring>
#include <iostream>
#include <stdexcept>

#include "log.h"
#include "utils.h"

MemoryPool::MemoryPool(size_t pool_size, size_t block_size, struct ibv_pd* pd)
    : pool_(nullptr),
      pool_size_(pool_size),
      block_size_(block_size),
      pd_(pd),
      mr_(nullptr),
      last_search_position_(0),
      allocated_blocks_(0) {
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

int MemoryPool::allocate(size_t size, size_t n, SimpleAllocationCallback callback) {
    size_t required_blocks = (size + block_size_ - 1) / block_size_;  // round up
    int num_allocated = 0;

    if (required_blocks > total_blocks_) {
        return 0;
    }

    size_t bit_per_word = 64;
    size_t shift = 6;

    for (size_t word_index = last_search_position_; word_index < bitmap_.size(); ++word_index) {
        if (num_allocated == n) {
            break;
        }

        uint64_t word = bitmap_[word_index];
        if (word == 0xFFFFFFFFFFFFFFFFULL) {
            continue;
        }
        for (size_t bit_index = __builtin_ctzll(~word); bit_index < bit_per_word; ++bit_index) {
            size_t start_block = (word_index << shift) + bit_index;

            if (start_block + required_blocks > total_blocks_) {
                allocated_blocks_ += num_allocated * required_blocks;
                return num_allocated;
            }

            bool found = true;
            for (size_t j = 0; j < required_blocks; ++j) {
                size_t idx = (start_block + j) >> shift;
                size_t bit = (start_block + j) & (bit_per_word - 1);
                if (bitmap_[idx] & (1ULL << bit)) {
                    found = false;
                    bit_index += j;  // skip all the blocks we already checked
                    break;
                }
            }

            if (found) {
                for (size_t j = 0; j < required_blocks; ++j) {
                    size_t idx = (start_block + j) >> shift;
                    size_t bit = (start_block + j) & (bit_per_word - 1);
                    bitmap_[idx] |= (1ULL << bit);
                }
                void* addr = static_cast<char*>(pool_) + start_block * block_size_;
                callback(addr, mr_->lkey, mr_->rkey);
                last_search_position_ = word_index;
                if (++num_allocated == n) {
                    break;
                }
            }
        }
    }

    allocated_blocks_ += num_allocated * required_blocks;
    return num_allocated;
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

void MM::add_mempool(struct ibv_pd* pd) {
    mempools_.push_back(new MemoryPool((size_t)EXTEND_POOL_SIZE, (size_t)EXTEND_BLOCK_SIZE, pd));
}

void MM::add_mempool(size_t pool_size, size_t block_size, struct ibv_pd* pd) {
    mempools_.push_back(new MemoryPool(pool_size, block_size, pd));
}

bool MM::allocate(size_t size, size_t n, AllocationCallback callback) {
    bool allocated = false;
    int mempool_cnt = mempools_.size();
    for (int i = 0; i < mempool_cnt; ++i) {
        // create a new callback from the original callback
        auto simple_callback = [callback, i](void* ptr, uint32_t lkey, uint32_t rkey) {
            callback(ptr, lkey, rkey, i);
        };

        int num_allocated = mempools_[i]->allocate(size, n, simple_callback);
        n -= num_allocated;

        auto total_blocks = mempools_[i]->get_total_blocks();
        auto allocated_blocks = mempools_[i]->get_allocated_blocks();
        DEBUG(
            "Mempool Count: {}, Pool idx: {}, Total blocks: {}, allocated blocks: {}, block usage: "
            "{}%",
            mempool_cnt, i, total_blocks, allocated_blocks, 100 * allocated_blocks / total_blocks);
        if (i == mempools_.size() - 1 &&
            (float)allocated_blocks / total_blocks > BLOCK_USAGE_RATIO) {
            need_extend = true;
        }
        if (n == 0) {
            allocated = true;
            break;
        }
    }
    return allocated;
}

void MM::deallocate(void* ptr, size_t size, int pool_idx) {
    mempools_[pool_idx]->deallocate(ptr, size);
}
