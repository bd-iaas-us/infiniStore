import torch
import _infinity


conn = _infinity.Connection()
if _infinity.init_connection(conn) != 0 :
    raise Exception("Failed to initialize connection")

def get(src_tensor, key):
    OP_R="R"
    OP_W="W"
    if src_tensor.device.type != "cuda":
        raise Exception("Tensor must be on CUDA device")
    ret = _infinity.rw_local(conn, OP_R, key, src_tensor.data_ptr(), src_tensor.numel() * src_tensor.element_size())
    if ret != 0:
        raise Exception("Failed to read tensor")

#t1 = torch.tensor([1, 2, 3], device="cuda", dtype=torch.float32)
t1 = torch.empty(3, device="cuda", dtype=torch.float32)
t2 = torch.empty(3, device="cuda", dtype=torch.float32)
#t2 = torch.tensor([1, 2, 3], device="cuda", dtype=torch.float32)

get(t1, "example_key")
print(t1)
get(t2, "test")
print(t2)




_infinity.close_connection(conn)
