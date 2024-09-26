from infinity.lib import InfinityConnection, DisableTorchCaching
import torch
import time
import requests

conn = InfinityConnection()
conn.connect()

# write 2GB data, 2 keys, block size 1GB
size = 1024*1024*1024 // 2
with DisableTorchCaching():
    src_tensor = torch.randn(size, device="cuda:0", dtype=torch.float32)

now = time.time()
conn.write_kvcache(src_tensor, [("key1", 0), ("key2", size//2)],  size//2)
print("1st: Submit 2GB task time: ", time.time() - now)

now = time.time()
conn.sync_local()
print("cuda stream sync ", time.time() - now)

# uncomment the following to verify reading the same data. with uncommented or cudaIpcCloseMemHandle, the sync_local data may not be accurate
# now = time.time()
# with DisableTorchCaching():
#     dst_tensor = torch.randn(size, device="cuda:1", dtype=torch.float32)
# conn.read_kvcache(dst_tensor, [("key1", 0), ("key2", size//2)], size//2)
# conn.sync_local()
# assert torch.equal(src_tensor[0:16].cpu(), dst_tensor[0:16].cpu())
# assert torch.equal(src_tensor[32:48].cpu(), dst_tensor[32:48].cpu())


resp = requests.get('http://127.0.0.1:8888/key/write/key1')
print(resp.text)

now = time.time()
src_tensor.cpu()
print("torch.Tensor.cpu time: ", time.time() - now)

print("done")