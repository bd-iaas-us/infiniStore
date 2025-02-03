import infinistore
import uuid
from infinistore import (
    register_server,
    purge_kv_map,
    get_kvmap_len,
    check_supported,
    ServerConfig,
    Logger,
)

import asyncio
import uvloop
from fastapi import FastAPI
import uvicorn
import torch
import argparse
import logging
import subprocess
import os


# disable standard logging, we will use our own logger
logging.disable(logging.INFO)

app = FastAPI()


@app.post("/purge")
async def purge():
    Logger.info("clear kvmap")
    num = get_kvmap_len()
    purge_kv_map()
    return {"status": "ok", "num": num}


def generate_uuid():
    return str(uuid.uuid4())


@app.post("/selftest/{number}")
async def selftest(number: int):
    Logger.info("selftest")

    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=number,
        log_level="info",
        connection_type=infinistore.TYPE_RDMA,
        ib_port=1,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="mlx5_2",
    )

    rdma_conn = infinistore.InfinityConnection(config)

    await rdma_conn.connect_async()

    def blocking_io(rdma_conn):
        src_tensor = torch.tensor(
            [i for i in range(4096)], device="cpu", dtype=torch.float32
        )
        dst_tensor = torch.zeros(4096, device="cpu", dtype=torch.float32)
        rdma_conn.register_mr(src_tensor)
        rdma_conn.register_mr(dst_tensor)
        return src_tensor, dst_tensor

    src_tensor, dst_tensor = await asyncio.to_thread(blocking_io, rdma_conn)

    # keys = ["key1", "key2", "key3"]
    keys = [generate_uuid() for i in range(3)]
    remote_addr = await rdma_conn.allocate_rdma_async(keys, 1024 * 4)
    print(f"remote addrs is {remote_addr}")

    await rdma_conn.rdma_write_cache_async(src_tensor, [0, 1024], 1024, remote_addr[:2])
    await rdma_conn.rdma_write_cache_async(src_tensor, [2048], 1024, remote_addr[2:])

    # # await asyncio.gather(rdma_conn.rdma_write_cache_async(src_tensor, [0, 1024], 1024, remote_addr[:2]),
    # #                rdma_conn.rdma_write_cache_async(src_tensor, [2048], 1024, remote_addr[2:]))

    await rdma_conn.read_cache_async(
        dst_tensor, [(keys[0], 0), (keys[1], 1024), (keys[2], 2048)], 1024
    )

    assert torch.equal(src_tensor[0:3072].cpu(), dst_tensor[0:3072].cpu())

    rdma_conn = None
    return {"status": "ok"}


@app.get("/kvmap_len")
async def kvmap_len():
    return {"len": get_kvmap_len()}


def check_p2p_access():
    num_devices = torch.cuda.device_count()
    for i in range(num_devices):
        for j in range(num_devices):
            if i != j:
                can_access = torch.cuda.can_device_access_peer(i, j)
                if can_access:
                    # print(f"Peer access supported between device {i} and {j}")
                    pass
                else:
                    Logger.warn(f"Peer access NOT supported between device {i} and {j}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--auto-increase",
        required=False,
        action="store_true",
        help="increase allocated memory automatically, 10GB each time, default False",
    )
    parser.add_argument(
        "--host",
        required=False,
        help="listen on which host, default 0.0.0.0",
        default="0.0.0.0",
        type=str,
    )
    parser.add_argument(
        "--manage-port",
        required=False,
        type=int,
        default=18080,
        help="port for control plane, default 18080",
    )
    parser.add_argument(
        "--service-port",
        required=False,
        type=int,
        default=22345,
        help="port for data plane, default 22345",
    )

    parser.add_argument(
        "--log-level",
        required=False,
        default="info",
        help="log level, default warning",
        type=str,
    )

    parser.add_argument(
        "--prealloc-size",
        required=False,
        type=int,
        default=16,
        help="prealloc mem pool size, default 16GB, unit: GB",
    )
    parser.add_argument(
        "--dev-name",
        required=False,
        default="mlx5_1",
        help="Use IB device <dev> (default first device found)",
        type=str,
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
        "--minimal-allocate-size",
        required=False,
        default=64,
        help="minimal allocate size, default 64, unit: KB",
        type=int,
    )
    parser.add_argument(
        "--num-stream",
        required=False,
        default=1,
        help="(deprecated)number of streams, default 1, can only be 1, 2, 4",
        type=int,
    )
    parser.add_argument(
        "--warmup",
        required=False,
        default=False,
        action="store_true",
    )

    return parser.parse_args()


def prevent_oom():
    pid = os.getpid()
    with open(f"/proc/{pid}/oom_score_adj", "w") as f:
        f.write("-1000")


def main():
    args = parse_args()
    config = ServerConfig(
        manage_port=args.manage_port,
        service_port=args.service_port,
        log_level=args.log_level,
        prealloc_size=args.prealloc_size,
        dev_name=args.dev_name,
        ib_port=args.ib_port,
        link_type=args.link_type,
        minimal_allocate_size=args.minimal_allocate_size,
        num_stream=args.num_stream,
        auto_increase=args.auto_increase,
    )
    config.verify()
    # check_p2p_access()
    check_supported()

    Logger.set_log_level(config.log_level)
    Logger.info(config)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    # 16 GB pre allocated
    # TODO: find the minimum size for pinning memory and ib_reg_mr
    register_server(loop, config)

    if args.warmup:
        Logger.info("Starting warm up all cuda devices, it may take a while...")
        subprocess.Popen(
            [
                "python",
                "-m",
                "infinistore.warmup",
                "--service-port",
                str(config.service_port),
                "--start-delay",
                "2",
            ]
        )

    prevent_oom()
    Logger.info("set oom_score_adj to -1000 to prevent OOM")

    http_config = uvicorn.Config(
        app, host="0.0.0.0", port=config.manage_port, loop="uvloop"
    )

    server = uvicorn.Server(http_config)

    Logger.warn("server started")
    loop.run_until_complete(server.serve())


if __name__ == "__main__":
    main()
