from infinity import _infinity
import torch

conn = _infinity.Connection()
_infinity.init_connection(conn)
_infinity.setup_rdma(conn)


# Create a tensor on cuda
a = torch.tensor([1, 2, 3, 4], device='cuda', dtype=torch.float32)
#int rw_remote(connection_t *conn, char op, const std::vector<std::string>keys, int block_size, void * ptr);
a.element_size()
_infinity.rw_remote(conn, 'W', ["Im_a_big_key"], a.element_size() * a.numel() , a.data_ptr())
_infinity.rw_remote(conn, 'W', ["Im_a_big_key1"], a.element_size() * a.numel() , a.data_ptr())
_infinity.rw_remote(conn, 'W', ["Im_a_big_key2"], a.element_size() * a.numel() , a.data_ptr())




