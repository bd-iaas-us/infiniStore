import infinistore
import asyncio
import uvloop
from fastapi import FastAPI
import uvicorn
import torch
import argparse

app = FastAPI()


@app.get("/kvmapSize")
async def read_status():
    return infinistore._infinistore.get_kvmap_len()


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
                    print(f"Peer access NOT supported between device {i} and {j}")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=False,
        help="listen on which host, default 0.0.0.0",
        default="0.0.0.0",
        type=str,
    )
    parser.add_argument(
        "--manage_port",
        required=False,
        default=18080,
        help="port for control plane, default 18080",
    )
    parser.add_argument(
        "--service_port",
        required=False,
        default=22345,
        help="port for data plane, default 22345",
    )
    parser.add_argument(
        "--log_level",
        required=False,
        default="warning",
        help="log level, default warning",
        type=str,
    )
    parser.add_argument(
        "--prealloc_size",
        required=False,
        default=16,
        help="prealloc mem pool size, default 16GB, unit: GB",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config = infinistore._infinistore.ServerConfig()
    config.manage_port, config.service_port, config.log_level, config.prealloc_size = (
        args.manage_port,
        args.service_port,
        args.log_level,
        args.prealloc_size,
    )

    check_p2p_access()
    infinistore.check_supported()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    # 16 GB pre allocated
    # TODO: find the minimum size for pinning memory and ib_reg_mr
    infinistore.register_server(loop, config)
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=config.manage_port,
        loop="uvloop",
        log_level=config.log_level,
    )

    server = uvicorn.Server(config)

    # 运行服务器
    loop.run_until_complete(server.serve())
