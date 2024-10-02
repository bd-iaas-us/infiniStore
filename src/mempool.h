#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <vector>
#include <cstddef>

#include <infiniband/verbs.h>
#include <cuda.h>
#include <cuda_runtime.h>


// c++ coding style: https://zh-google-styleguide.readthedocs.io/en/latest/google-cpp-styleguide/naming.html#variable-names

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

#endif // MEMORY_POOL_H
