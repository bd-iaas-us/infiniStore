import _infinity
import torch


conn = _infinity.Connection()
if _infinity.init_connection(conn) < 0:
    raise Exception("Failed to initialize connection")

key = "example_key"
tensor = torch.tensor([0, 0, 0], device="cuda", dtype=torch.float32)
OP_R="R"
OP_W="W"
_infinity.rw_local(conn, OP_R, key, tensor.data_ptr(), tensor.numel() * tensor.element_size())
print(f"read tensor {tensor} to infinity with key {key}")
_infinity.close_connection(conn)
