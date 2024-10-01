from infinity import _infinity
import torch

conn = _infinity.Connection()
_infinity.init_connection(conn, "127.0.0.1")
_infinity.setup_rdma(conn)


#copy from 
OP_R="R"
OP_W="W"
OP_SYNC="S"
OP_RDMA_EXCHANGE="E"
OP_RDMA_WRITE="D"
OP_RDMA_READ="A"

# Create a tensor on cuda
# 256 failed?
a = torch.zeros(1024, device='cuda', dtype=torch.float32)
for i in range(a.numel()):
    a[i] = i
size = a.element_size() * a.numel()
_infinity.rw_rdma(conn, OP_RDMA_WRITE, [("Im_a_big_key", 0), ("Im_a_small_key", size//2)], size//2, a.data_ptr())

_infinity.sync_rdma(conn)
print(a[-1])

b = torch.zeros(512, device='cuda:2', dtype=torch.float32)
_infinity.rw_rdma(conn, OP_RDMA_READ, [("Im_a_small_key", 0)], size//2 , b.data_ptr())
_infinity.sync_rdma(conn)

print(b)

