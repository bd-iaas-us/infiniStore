from infinistore import (
    ClientConfig,
    check_supported,
    DisableTorchCaching,
    InfinityConnection,
)
import infinistore

import torch

# import time


def generate_random_string(length):
    import string
    import random

    letters_and_digits = string.ascii_letters + string.digits
    random_string = "".join(random.choice(letters_and_digits) for i in range(length))
    return random_string


def run(conn):
    check_supported()
    # a = [generate_random_string(8) for i in range(4096)]
    # a = ["a", "b", "c"]
    src = [i for i in range(4096)]
    with DisableTorchCaching():  # not required if using RDMA
        src_tensor = torch.tensor(src, device="cuda", dtype=torch.float32)

    conn.register_mr(src_tensor)
    # now = time.time()
    x = conn.allocate_rdma(["a", "b", "c"], 1024 * 4)
    y = conn.allocate_rdma(["d", "e", "f"], 1024 * 4)
    conn.rdma_write_cache(src_tensor, [0, 1024, 2048], 1024, x)
    conn.rdma_write_cache(src_tensor, [2048, 2048 + 1024], 1024, y[:2])

    # print(x)

    conn.rdma_write_cache(src_tensor, [0, 1024, 2048], 1024, x)
    # # print(src_tensor[0:10])
    print("1")

    z = conn.allocate_rdma(["d", "e", "f"], 1024 * 4)
    print("2")

    conn.rdma_write_cache(src_tensor, [0, 1024, 2048], 1024, z)
    print("3")

    conn.sync()

    # print(x)
    # print(f"write elapse time is {time.time() - now}")

    dst_tensor = torch.zeros(4096, device="cuda", dtype=torch.float32)
    conn.register_mr(dst_tensor)
    conn.read_cache(dst_tensor, [("a", 0), ("b", 1024)], 1024)

    conn.sync()

    print(dst_tensor[0:100])

    # src = [i for i in range(4096)]
    # with DisableTorchCaching():  # not required if using RDMA
    #     src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)
    # if conn.rdma_connected:
    #     conn.register_mr(src_tensor)
    # now = time.time()
    # conn.write_cache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
    # print(f"write elapse time is {time.time() - now}")

    # before_sync = time.time()
    # conn.sync()
    # print(f"sync elapse time is {time.time() - before_sync}")

    # with DisableTorchCaching():
    #     dst_tensor = torch.zeros(4096, device="cuda:2", dtype=torch.float32)
    # if conn.rdma_connected:
    #     conn.register_mr(dst_tensor)
    # now = time.time()
    # conn.read_cache(dst_tensor, [("key1", 0), ("key2", 1024)], 1024)
    # # conn.read_cache(dst_tensor, [("key1", 0)], 1024)

    # conn.sync()
    # print(f"read elapse time is {time.time() - now}")

    # assert torch.equal(src_tensor[0:1024].cpu(), dst_tensor[0:1024].cpu())
    # assert torch.equal(src_tensor[1024:2048].cpu(), dst_tensor[1024:2048].cpu())


if __name__ == "__main__":
    config = ClientConfig(
        host_addr="127.0.0.1",
        service_port=12345,
        log_level="debug",
        connection_type=infinistore.TYPE_RDMA,
        ib_port=1,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_0",
    )
    rdma_conn = InfinityConnection(config)
    rdma_conn.connect()
    run(rdma_conn)

    # config.connection_type = infinistore.TYPE_LOCAL_GPU
    # local_conn = InfinityConnection(config)
    # local_conn.connect()
    # run(local_conn)
