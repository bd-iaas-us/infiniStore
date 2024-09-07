import _infinity
import torch
import ctypes


conn = _infinity.Connection()
if _infinity.init_connection(conn) < 0:
    raise Exception("Failed to initialize connection")

key = "example_key"
src_tensor = torch.tensor([1, 2, 3], device="cuda", dtype=torch.float32)

if src_tensor.device.type != "cuda":
    raise Exception("Tensor must be on CUDA device")

#import pdb; pdb.set_trace()
assert src_tensor.is_contiguous()
OP_R="R"
OP_W="W"

ret = _infinity.rw_local(conn, OP_W, key, src_tensor.data_ptr(), src_tensor.numel() * src_tensor.element_size())


_infinity.close_connection(conn)
print(f"write tensor {src_tensor} to infinity with key {key}, ret = {ret}")




key = "example_key"
OP_R="R"
OP_W="W"
conn = _infinity.Connection()

ret = _infinity.init_connection(conn)

dst_tensor = torch.tensor([0, 0, 0], device="cuda", dtype=torch.float32)
ret = _infinity.rw_local(conn, OP_R, key, dst_tensor.data_ptr(), dst_tensor.numel() * dst_tensor.element_size())
print(f"read tensor {dst_tensor} from infinity with key {key} ret = {ret}")
_infinity.close_connection(conn)