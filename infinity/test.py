from infinity.lib import InfinityConnection, DisableTorchCaching
import torch


conn = InfinityConnection()
conn.connect()

key = "example_key"

src = [i for i in range(1024)]
with DisableTorchCaching():
    src_tensor = torch.tensor(src, device="cuda", dtype=torch.float32)


conn.write_kvcache(src_tensor, [("key1", 0), ("key2", 32)], 16)
