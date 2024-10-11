from infinity.lib import InfinityConnection, DisableTorchCaching, check_infinity_supported
import torch
import time

def run(conn):
    check_infinity_supported()
    src = [i for i in range(4096)]
    # with DisableTorchCaching():
    #     src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)
    
    src_tensor = torch.zeros(268435456, device="cuda:0", dtype=torch.float32)
    print("build tensor done")
    now=time.time()
    conn.write_cache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
    conn.sync()
    print(f"write elapse time is {time.time() - now}")


    # with DisableTorchCaching():
    #     dst_tensor = torch.zeros(4096, device="cuda:2", dtype=torch.float32)

    # now=time.time()
    # conn.read_cache(dst_tensor, [("key1", 0), ("key2", 1024)], 1024)
    # conn.sync()
    # print(f"read elapse time is {time.time() - now}")


    # assert torch.equal(src_tensor[0:1024].cpu(), dst_tensor[0:1024].cpu())

    # assert torch.equal(src_tensor[1024:2048].cpu(), dst_tensor[1024:2048].cpu())


if __name__ == "__main__":
    rdma_conn = InfinityConnection()
    rdma_conn.connect("127.0.0.1")
    run(rdma_conn)

    # local_conn = InfinityConnection()
    # local_conn.local_connect()
    # run(local_conn)