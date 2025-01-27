# python benchmark.py --service-port 12345 --dev-name mlx5_0 --link-type Ethernet --size 100 --block-size 32 --src-gpu 1 --dst-gpu  0 --iteration 1 --rdma

import infinistore
import torch
import time
import random
import string
import argparse
import uuid


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rdma",
        required=False,
        action="store_true",
        help="use rdma connection, default False",
    )

    parser.add_argument(
        "--server",
        required=False,
        help="connect to which server, default 127.0.0.1",
        default="127.0.0.1",
        type=str,
    )
    parser.add_argument(
        "--service-port",
        required=False,
        type=int,
        default=22345,
        help="port for data plane, default 22345",
    )
    parser.add_argument(
        "--dev-name",
        required=False,
        default="mlx5_1",
        help="Use IB device <dev> (default first device found)",
        type=str,
    )
    parser.add_argument(
        "--iteration",
        required=False,
        type=int,
        default=1,
        help="number of iterations, default 100",
    )
    parser.add_argument(
        "--block-size",
        required=False,
        type=int,
        default=32,
        help="block size, unit: KB, default 32",
    )
    parser.add_argument(
        "--size",
        required=False,
        type=int,
        default=128,
        help="size for benchmarking, unit: MB, default 128",
    )
    parser.add_argument(
        "--src-gpu",
        required=False,
        type=int,
        default=0,
        help="gpu# for data write from, default 0",
    )
    parser.add_argument(
        "--dst-gpu",
        required=False,
        type=int,
        default=1,
        help="gpu# for data read to, default 1",
    )
    parser.add_argument(
        "--ib-port",
        required=False,
        type=int,
        default=1,
        help="use port <port> of IB device (default 1)",
    )
    parser.add_argument(
        "--link-type",
        required=False,
        default="IB",
        help="IB or Ethernet, default IB",
        type=str,
    )
    parser.add_argument(
        "--steps",
        required=False,
        type=int,
        default=32,
        help="number of steps, default 32",
    )
    return parser.parse_args()


def generate_random_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    random_string = "".join(random.choice(letters_and_digits) for i in range(length))
    return random_string


def generate_uuid():
    return str(uuid.uuid4())


def run(args):
    config = infinistore.ClientConfig(
        host_addr=args.server,
        service_port=args.service_port,
        dev_name=args.dev_name,
        ib_port=args.ib_port,
        link_type=args.link_type,
        log_level="warning",
    )

    config.connection_type = (
        infinistore.TYPE_RDMA if args.rdma else infinistore.TYPE_LOCAL_GPU
    )

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    src_device = "cuda:" + str(args.src_gpu)
    dst_device = "cuda:" + str(args.dst_gpu)

    block_size = args.block_size * 1024 // 4
    num_of_blocks = args.size * 1024 * 1024 // (args.block_size * 1024)

    with infinistore.DisableTorchCaching():
        src_tensor = torch.rand(
            num_of_blocks * block_size, device=src_device, dtype=torch.float32
        )

    with infinistore.DisableTorchCaching():
        dst_tensor = torch.rand(
            num_of_blocks * block_size, device=dst_device, dtype=torch.float32
        )

    torch.cuda.synchronize(src_tensor.device)
    torch.cuda.synchronize(dst_tensor.device)
    if args.rdma:
        conn.register_mr(src_tensor)
        conn.register_mr(dst_tensor)

    # blocks = [(keys[i], offset_blocks[i]) for i in range(num_of_blocks)]
    write_sum = 0.0
    read_sum = 0.0

    for _ in range(args.iteration):
        keys = [generate_uuid() for i in range(num_of_blocks)]
        offset_blocks = [i * block_size for i in range(num_of_blocks)]
        # zip keys and offset_blocks
        blocks = list(zip(keys, offset_blocks))

        if args.rdma:
            remote_addrs = conn.allocate_rdma(keys, block_size * 4)

        steps = args.steps
        # simulate we have <steps> layers, this steps should be less then MAX_WR_SIZE
        while len(blocks) % steps != 0 and steps > 1:
            steps = int(steps / 2)
        print(f"\nSimulate {steps} layers, running\n")
        # how many blocks to write in each step
        n = int(len(blocks) / steps)

        start = time.time()

        for i in range(steps):
            if args.rdma:
                conn.rdma_write_cache(
                    src_tensor,
                    offset_blocks[i * n : i * n + n],
                    block_size,
                    remote_addrs[i * n : i * n + n],
                )
            else:
                conn.local_gpu_write_cache(
                    src_tensor, blocks[i * n : i * n + n], block_size
                )
        conn.sync()
        # print(f"write  takes {time.time() - start} seconds")

        mid = time.time()
        write_sum += mid - start
        for i in range(steps):
            conn.read_cache(dst_tensor, blocks[i * n : i * n + n], block_size)

        conn.sync()
        end = time.time()
        read_sum += end - mid

    print(
        "size: {} MB, block size: {} KB, connection type: {}".format(
            args.size * args.iteration, args.block_size, config.connection_type
        )
    )
    print(
        "write cache: {:.2f} MB/s, read cache: {:.2f} MB/s".format(
            args.size * args.iteration / write_sum,
            args.size * args.iteration / read_sum,
        )
    )

    assert torch.equal(src_tensor.cpu(), dst_tensor.cpu())


if __name__ == "__main__":
    args = parse_args()
    run(args)
