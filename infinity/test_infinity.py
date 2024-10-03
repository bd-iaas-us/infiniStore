from infinity.lib import InfinityConnection, DisableTorchCaching
import torch
import time

def test_local():
    local_conn = InfinityConnection()
    local_conn.local_connect()
    run(local_conn)
    print("test_local done")

def test_remote():
    rdma_conn = InfinityConnection()
    rdma_conn.connect("10.192.24.218")
    run(rdma_conn)
    print("test_remote done")


def run(conn):
    src = [i for i in range(4096)]
    with DisableTorchCaching():
        src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)
    now=time.time()
    conn.write_cache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
    print("sync: ", conn.sync())
    print(f"write elapse time is {time.time() - now}")

    time.sleep(3)
    print("sleep 3s, sync: ", conn.sync())


    with DisableTorchCaching():
        dst_tensor = torch.zeros(4096, device="cuda:2", dtype=torch.float32)


    conn.read_cache(dst_tensor, [("key1", 0), ("key2", 1024)], 1024)
    print("sync: ", conn.sync())

    time.sleep(3)
    print("sleep 3s, sync: ", conn.sync())    

    assert torch.equal(src_tensor[0:1024].cpu(), dst_tensor[0:1024].cpu())

    assert torch.equal(src_tensor[1024:2048].cpu(), dst_tensor[1024:2048].cpu())

test_local()