from infinity.lib import InfinityConnection, DisableTorchCaching
import torch
import time


conn = InfinityConnection()
conn.connect()

src = [i for i in range(4096)]
with DisableTorchCaching():
    src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)

#write 16 elements

# conn.write_kvcache(src_tensor, [("key1", 0), ("key2", 32), ("key3", 32)], 16)

# conn.sync_local()

conn.write_kvcache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
conn.sync_local()

# conn.remote_write_kvcache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
# conn.sync_remote()


with DisableTorchCaching():
    dst_tensor = torch.zeros(4096, device="cuda:2", dtype=torch.float32)


conn.remote_read_kvcache(dst_tensor, [("key1", 0), ("key2", 1024)], 1024)
conn.sync_remote()


assert torch.equal(src_tensor[0:1024].cpu(), dst_tensor[0:1024].cpu())

assert torch.equal(src_tensor[1024:2048].cpu(), dst_tensor[1024:2048].cpu())