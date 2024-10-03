#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <vector>
#include <cstddef>
#include <infiniband/verbs.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <assert.h>


class MemoryPool {
public:
    MemoryPool(size_t pool_size, size_t block_size, struct ibv_pd* pd);

    ~MemoryPool();

    /*
    @brief size should be aligned to block size
    */
    void* allocate(size_t size);
    /*
    @brief size should be aligned to block size
    */
    void deallocate(void* ptr, size_t size);

    uint32_t get_rkey() const { return mr_->rkey; }

private:
    void* pool_;                
    size_t pool_size_;           
    size_t block_size_;          
    size_t total_blocks_;        

    //TODO: use judy libray to speed up the bitmap?
    std::vector<uint64_t> bitmap_;  

    struct ibv_mr* mr_;        
    struct ibv_pd* pd_;         
};

class MM {
    private:
        std::vector<MemoryPool*> mempools_;
    public:
        MM(size_t pool_size, size_t block_size, struct ibv_pd* pd) {
            mempools_.push_back(new MemoryPool(pool_size, block_size, pd));
        }
        MM(const MM& mm) = delete;
        void * allocate(size_t size, int *pool_idx);
        void deallocate(void* ptr, size_t size, int pool_idx);
        uint32_t get_rkey(int pool_idx) const { 
            assert(pool_idx >= 0 && pool_idx < mempools_.size());
            return mempools_[pool_idx]->get_rkey(); 
        }

        ~MM() {
            for (auto &pool : mempools_) {
                delete pool;
            }
        }
};

#endif // MEMORY_POOL_H
