#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <assert.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <infiniband/verbs.h>
#include <jemalloc/jemalloc.h>

#include <cstddef>
#include <functional>
#include <vector>

using AllocationCallback =
    std::function<void(void* ptr, uint32_t lkey, uint32_t rkey, int pool_idx)>;
using SimpleAllocationCallback = std::function<void(void* ptr, uint32_t lkey, uint32_t rkey)>;

class MemoryPool {
   public:
    virtual ~MemoryPool() = default;
    virtual bool allocate(size_t size, size_t n, SimpleAllocationCallback callback) = 0;
    virtual void deallocate(void* ptr, size_t size) = 0;
    virtual uint32_t get_lkey() const = 0;
    virtual uint32_t get_rkey() const = 0;
};

class BitmapMemoryPool : public MemoryPool {
   public:
    BitmapMemoryPool(size_t pool_size, size_t block_size, struct ibv_pd* pd);

    ~BitmapMemoryPool();

    /*
    @brief size should be aligned to block size
    */
    bool allocate(size_t size, size_t n, SimpleAllocationCallback callback);
    /*
    @brief size should be aligned to block size
    */
    void deallocate(void* ptr, size_t size);

    uint32_t get_lkey() const { return mr_->lkey; }
    uint32_t get_rkey() const { return mr_->rkey; }

   private:
    void* pool_;
    size_t pool_size_;
    size_t chunk_size_;
    size_t total_chunks_;

    // TODO: use judy libray to speed up the bitmap?
    std::vector<uint64_t> bitmap_;

    struct ibv_mr* mr_;
};

class JEMemoryPool : public MemoryPool {
   public:
    JEMemoryPool(size_t pool_size, struct ibv_pd* pd);
    ~JEMemoryPool() override;
    bool allocate(size_t size, size_t n, SimpleAllocationCallback callback) override;
    void deallocate(void* ptr, size_t size) override;
    uint32_t get_lkey() const override { return mr_->lkey; }
    uint32_t get_rkey() const override { return mr_->rkey; }

   private:
    // void * custom_extent_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size,
    //                    size_t alignment, bool *zero, bool *commit, unsigned arena_ind);
    size_t pool_size_;
    unsigned arena_ind_;
    struct ibv_mr* mr_;
};

class MM {
   private:
    std::vector<MemoryPool*> mempools_;

   public:
    MM(size_t pool_size, size_t block_size, struct ibv_pd* pd) {
        mempools_.push_back(new JEMemoryPool(pool_size, pd));
    }
    MM(const MM& mm) = delete;
    bool allocate(size_t size, size_t n, AllocationCallback callback);
    void deallocate(void* ptr, size_t size, int pool_idx);
    uint32_t get_lkey(int pool_idx) const {
        assert(pool_idx >= 0 && pool_idx < mempools_.size());
        return mempools_[pool_idx]->get_lkey();
    }
    uint32_t get_rkey(int pool_idx) const {
        assert(pool_idx >= 0 && pool_idx < mempools_.size());
        return mempools_[pool_idx]->get_rkey();
    }

    ~MM() {
        for (auto& pool : mempools_) {
            delete pool;
        }
    }
};

#endif  // MEMORY_POOL_H
