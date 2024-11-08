from infinistore import (
    ClientConfig,
    check_supported,
    DisableTorchCaching,
    InfinityConnection,
)
import infinistore
import torch
import time


def run(conn):
    check_supported()
    src = [i for i in range(4096)]
    with DisableTorchCaching():  # not required if using RDMA
        src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)
    if conn.rdma_connected:
        conn.register_mr(src_tensor)
    now = time.time()
    conn.write_cache(src_tensor, [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024)
    # conn.write_cache(src_tensor, [("key1", 0)], 1024)

    conn.sync()
    print(f"write elapse time is {time.time() - now}")

    with DisableTorchCaching():
        dst_tensor = torch.zeros(4096, device="cuda:2", dtype=torch.float32)
    if conn.rdma_connected:
        conn.register_mr(dst_tensor)
    now = time.time()
    conn.read_cache(dst_tensor, [("key1", 0), ("key2", 1024)], 1024)
    conn.sync()
    print(f"read elapse time is {time.time() - now}")

    assert torch.equal(src_tensor[0:1024].cpu(), dst_tensor[0:1024].cpu())

    assert torch.equal(src_tensor[1024:2048].cpu(), dst_tensor[1024:2048].cpu())


if __name__ == "__main__":
    config = ClientConfig(
        host_addr="127.0.0.1",
        service_port=22345,
        log_level="debug",
        connection_type=infinistore.TYPE_RDMA,
        ib_port=1,
        link_type="IB",
        dev_name="mlx5_1",
    )
    rdma_conn = InfinityConnection(config)
    rdma_conn.connect()
    run(rdma_conn)

    config.connection_type = infinistore.TYPE_LOCAL_GPU
    local_conn = InfinityConnection(config)
    local_conn.connect()
    run(local_conn)
