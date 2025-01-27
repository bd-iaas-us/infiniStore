import torch
import pytest
import infinistore
import time
import os
import signal
import subprocess
import random
import string
import contextlib
from multiprocessing import Process


# Fixture to start the TCzpserver before running tests
@pytest.fixture(scope="module")
def server():
    server_process = subprocess.Popen(
        [
            "python",
            "-m",
            "infinistore.server",
            "--dev-name",
            "mlx5_2",
            "--link-type",
            "Ethernet",
            "--service-port",
            "92345",
            "--manage-port",
            "98080",
        ]
    )
    time.sleep(4)
    if server_process.poll() is None:
        print("Test Server process is running.")
    else:
        print("Server process failed to start or has already exited.")
        assert False
    yield
    os.kill(server_process.pid, signal.SIGINT)
    server_process.wait()


# add a flat to wehther the same connection.


def generate_random_string(length):
    letters_and_digits = string.ascii_letters + string.digits  # 字母和数字的字符集
    random_string = "".join(random.choice(letters_and_digits) for i in range(length))
    return random_string


def get_gpu_count():
    if torch.cuda.is_available():
        gpu_count = torch.cuda.device_count()
        return gpu_count
    else:
        return 0


@pytest.mark.parametrize("dtype", [torch.float16, torch.float32])
@pytest.mark.parametrize("new_connection", [True, False])
@pytest.mark.parametrize("local", [True, False])
def test_basic_read_write_cache(server, dtype, new_connection, local):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
    )

    config.connection_type = (
        infinistore.TYPE_LOCAL_GPU if local else infinistore.TYPE_RDMA
    )

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    # key is random string
    key = generate_random_string(10)
    src = [i for i in range(4096)]

    # local GPU write is tricky, we need to disable the pytorch allocator's caching
    with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
        src_tensor = torch.tensor(src, device="cuda:0", dtype=dtype)

    torch.cuda.synchronize(src_tensor.device)
    if not local:
        conn.register_mr(src_tensor)
        element_size = torch._utils._element_size(dtype)

        remote_addrs = conn.allocate_rdma([key], 4096 * element_size)
        conn.rdma_write_cache(src_tensor, [0], 4096, remote_addrs)
    else:
        conn.local_gpu_write_cache(src_tensor, [(key, 0)], 4096)

    conn.sync()

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
        dst = torch.zeros(4096, device="cuda:0", dtype=dtype)
    if not local:
        conn.register_mr(dst)
    conn.read_cache(dst, [(key, 0)], 4096)
    conn.sync()
    assert torch.equal(src_tensor, dst)


@pytest.mark.parametrize("seperated_gpu", [False, True])
@pytest.mark.parametrize("local", [False])
def test_batch_read_write_cache(server, seperated_gpu, local):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
    )

    config.connection_type = (
        infinistore.TYPE_LOCAL_GPU if local else infinistore.TYPE_RDMA
    )
    # test if we have multiple GPUs
    if seperated_gpu:
        if get_gpu_count() >= 2:
            src_device = "cuda:0"
            dst_device = "cuda:1"
        else:
            # skip if we don't have enough GPUs
            return
    else:
        src_device = "cuda:0"
        dst_device = "cuda:0"

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    num_of_blocks = 10
    block_size = 4096
    src = [i for i in range(num_of_blocks * block_size)]

    with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
        src_tensor = torch.tensor(src, device=src_device, dtype=torch.float32)
    torch.cuda.synchronize(src_tensor.device)

    # write/read 3 times
    for i in range(3):
        keys = [generate_random_string(num_of_blocks) for i in range(10)]
        blocks = [(keys[i], i * block_size) for i in range(num_of_blocks)]
        if not local:
            conn.register_mr(src_tensor)
            remote_addrs = conn.allocate_rdma(keys, block_size * 4)
            conn.rdma_write_cache(
                src_tensor,
                [i * block_size for i in range(num_of_blocks)],
                block_size,
                remote_addrs,
            )
        else:
            conn.local_gpu_write_cache(src_tensor, blocks, block_size)

        conn.sync()

        with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
            dst = torch.zeros(
                num_of_blocks * block_size, device=dst_device, dtype=torch.float32
            )

        if not local:
            conn.register_mr(dst)
        conn.read_cache(dst, blocks, block_size)
        conn.sync()
        # import pdb; pdb.set_trace()
        assert torch.equal(src_tensor.cpu(), dst.cpu())


@pytest.mark.parametrize("num_clients", [2])
@pytest.mark.parametrize("local", [True, False])
def test_multiple_clients(num_clients, local):
    def run():
        config = infinistore.ClientConfig(
            host_addr="127.0.0.1",
            service_port=92345,
            link_type=infinistore.LINK_ETHERNET,
            dev_name="mlx5_2",
        )

        config.connection_type = (
            infinistore.TYPE_LOCAL_GPU if local else infinistore.TYPE_RDMA
        )

        conn = infinistore.InfinityConnection(config)
        conn.connect()

        # key is random string
        key = generate_random_string(10)
        src = [i for i in range(4096)]

        # local GPU write is tricky, we need to disable the pytorch allocator's caching
        with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
            src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)

        torch.cuda.synchronize(src_tensor.device)
        if not local:
            conn.register_mr(src_tensor)
            element_size = torch._utils._element_size(torch.float32)

            remote_addrs = conn.allocate_rdma([key], 4096 * element_size)
            conn.rdma_write_cache(src_tensor, [0], 4096, remote_addrs)
        else:
            conn.local_gpu_write_cache(src_tensor, [(key, 0)], 4096)

        conn.sync()

        conn = infinistore.InfinityConnection(config)
        conn.connect()

        with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
            dst = torch.zeros(4096, device="cuda:0", dtype=torch.float32)
        if not local:
            conn.register_mr(dst)
        conn.read_cache(dst, [(key, 0)], 4096)
        conn.sync()
        assert torch.equal(src_tensor, dst)

    processes = []
    for _ in range(num_clients):
        p = Process(target=run)
        p.start()
        processes.append(p)
    for i in range(num_clients):
        processes[i].join()


def test_key_check(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)
    conn.connect()
    key = generate_random_string(5)
    src = torch.randn(4096, device="cuda", dtype=torch.float32)
    conn.register_mr(src)
    remote_addrs = conn.allocate_rdma([key], 4096 * 4)

    torch.cuda.synchronize(src.device)

    conn.rdma_write_cache(src, [0], 4096, remote_addrs)
    conn.sync()
    assert conn.check_exist(key)


def test_get_match_last_index(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)
    conn.connect()
    src = torch.randn(4096, device="cuda", dtype=torch.float32)
    conn.register_mr(src)
    remote_addrs = conn.allocate_rdma(["key1", "key2", "key3"], 4096 * 4)

    torch.cuda.synchronize(src.device)

    conn.rdma_write_cache(src, [0, 1024, 2048], 4096, remote_addrs)
    assert conn.get_match_last_index(["A", "B", "C", "key1", "D", "E"]) == 3


def test_key_not_found(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
        connection_type=infinistore.TYPE_LOCAL_GPU,
    )
    conn = infinistore.InfinityConnection(config)

    conn.connect()
    key = "not_exist_key"
    src = torch.randn(4096, device="cuda", dtype=torch.float32)
    # expect raise exception
    with pytest.raises(Exception):
        conn.read_cache(src, [(key, 0)], 4096)


def test_upload_cpu_download_gpu(server):
    src_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_0",
        connection_type=infinistore.TYPE_RDMA,
    )
    dst_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        connection_type=infinistore.TYPE_LOCAL_GPU,
    )
    src_conn = infinistore.InfinityConnection(src_config)
    src_conn.connect()

    key = generate_random_string(5)
    src = torch.randn(4096, dtype=torch.float32, device="cpu")
    # NOTE: not orch.cuda.synchronize required for CPU tensor
    src_conn.register_mr(src)
    remote_addrs = src_conn.allocate_rdma([key], 4096 * 4)
    src_conn.rdma_write_cache(src, [0], 4096, remote_addrs)
    src_conn.sync()

    dst_conn = infinistore.InfinityConnection(dst_config)
    dst_conn.connect()

    dst = torch.zeros(4096, dtype=torch.float32, device="cuda:0")
    dst_conn.read_cache(dst, [(key, 0)], 4096)
    dst_conn.sync()
    assert torch.equal(src, dst.cpu())


@pytest.mark.parametrize("local", [False])
def test_deduplicate(server, local):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
    )

    config.connection_type = (
        infinistore.TYPE_LOCAL_GPU if local else infinistore.TYPE_RDMA
    )

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    key = "duplicate_key"
    src = [i for i in range(4096)]
    with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
        src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)

    torch.cuda.synchronize(src_tensor.device)
    if not local:
        conn.register_mr(src_tensor)
        element_size = torch._utils._element_size(torch.float32)

        remote_addrs = conn.allocate_rdma([key], 4096 * element_size)
        print(remote_addrs)
        conn.rdma_write_cache(src_tensor, [0], 4096, remote_addrs)
    else:
        conn.local_gpu_write_cache(src_tensor, [(key, 0)], 4096)

    conn.sync()

    with infinistore.DisableTorchCaching() if local else contextlib.nullcontext():
        src2_tensor = torch.randn(4096, device="cuda:0", dtype=torch.float32)

    # test_deduplicate
    if not local:
        conn.register_mr(src2_tensor)
        element_size = torch._utils._element_size(torch.float32)

        remote_addrs = conn.allocate_rdma([key], 4096 * element_size)
        conn.rdma_write_cache(src_tensor, [0], 4096, remote_addrs)
    else:
        conn.local_gpu_write_cache(src_tensor, [(key, 0)], 4096)
    conn.sync()

    if not local:
        dst_tensor = torch.zeros(4096, dtype=torch.float32, device="cpu")
        conn.register_mr(dst_tensor)
    else:
        dst_tensor = torch.zeros(4096, dtype=torch.float32, device="cuda:0")

    conn.read_cache(dst_tensor, [(key, 0)], 4096)
    conn.sync()

    assert torch.equal(src_tensor.cpu(), dst_tensor.cpu())
    assert not torch.equal(src2_tensor.cpu(), dst_tensor.cpu())
