from .lib import register_server, check_supported, ServerConfig, Logger
from ._infinistore import get_kvmap_len

import asyncio
import uvloop
from fastapi import FastAPI
import uvicorn
import torch
import argparse

import logging

logging.disable(logging.INFO)

app = FastAPI()


@app.get("/kvmapSize")
async def read_status():
    return get_kvmap_len()


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
        default="Ethernet",
        help="IB or Ethernet, default Ethernet",
        type=str,
    )
    return parser.parse_args()


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
    )
    config.verify()
    check_p2p_access()
    check_supported()

    Logger.set_log_level(config.log_level)
    Logger.info(config)
    # print error
    # Logger.error("test error", sys._getframe(1).f_lineno, __file__)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    # 16 GB pre allocated
    # TODO: find the minimum size for pinning memory and ib_reg_mr
    register_server(loop, config)

    http_config = uvicorn.Config(
        app, host="0.0.0.0", port=config.manage_port, loop="uvloop"
    )

    server = uvicorn.Server(http_config)

    loop.run_until_complete(server.serve())


if __name__ == "__main__":
    main()
