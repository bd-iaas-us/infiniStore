import torch
import pytest
import infinity
import time
import os
import signal
import subprocess
import random
import string
import contextlib


# Fixture to start the TCzpserver before running tests
@pytest.fixture(scope="module")
def server():
    server_process = subprocess.Popen(["python", "start.py"])
    time.sleep(4)
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
    conn = infinity.InfinityConnection()
    if local:
        conn.local_connect()
    else:
        conn.connect("127.0.0.1")

    # key is random string
    key = generate_random_string(10)
    src = [i for i in range(4096)]

    # local GPU write is tricky, we need to disable the pytorch allocator's caching
    with infinity.DisableTorchCaching() if local else contextlib.nullcontext():
        src_tensor = torch.tensor(src, device="cuda:0", dtype=dtype)

    conn.write_cache(src_tensor, [(key, 0)], 4096)
    conn.sync()

    conn = infinity.InfinityConnection()
    conn.local_connect() if local else conn.connect("127.0.0.1")

    with infinity.DisableTorchCaching() if local else contextlib.nullcontext():
        dst = torch.zeros(4096, device="cuda:0", dtype=dtype)
    conn.read_cache(dst, [(key, 0)], 4096)
    conn.sync()
    assert torch.equal(src_tensor, dst)


@pytest.mark.parametrize("seperated_gpu", [True, False])
@pytest.mark.parametrize("local", [True, False])
def test_batch_read_write_cache(server, seperated_gpu, local):
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

    conn = infinity.InfinityConnection()

    conn.local_connect() if local else conn.connect("127.0.0.1")

    num_of_blocks = 10
    keys = [generate_random_string(num_of_blocks) for i in range(10)]
    block_size = 4096
    src = [i for i in range(num_of_blocks * block_size)]

    with infinity.DisableTorchCaching() if local else contextlib.nullcontext():
        src_tensor = torch.tensor(src, device=src_device, dtype=torch.float32)

    blocks = [(keys[i], i * block_size) for i in range(num_of_blocks)]

    conn.write_cache(src_tensor, blocks, block_size)
    conn.sync()

    with infinity.DisableTorchCaching() if local else contextlib.nullcontext():
        dst = torch.zeros(
            num_of_blocks * block_size, device=dst_device, dtype=torch.float32
        )

    conn.read_cache(dst, blocks, block_size)
    conn.sync()
    # import pdb; pdb.set_trace()
    assert torch.equal(src_tensor.cpu(), dst.cpu())
