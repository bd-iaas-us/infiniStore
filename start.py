import infinity
import asyncio
import uvloop
import fastapi
from fastapi import FastAPI, Request
import uvicorn
import torch
import argparse

app = FastAPI()

@app.get("/kvmapSize")
async def read_status():
    return infinity._infinity.get_kvmap_len()

def check_p2p_access():
    num_devices = torch.cuda.device_count()
    for i in range(num_devices):
        for j in range(num_devices):
            if i != j:
                can_access = torch.cuda.can_device_access_peer(i, j)
                if can_access:
                    #print(f"Peer access supported between device {i} and {j}")
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
        "--port",
        required=False,
        default=18080,
        help="Listen on which port, default 18080",
    )
    parser.add_argument(
        "--log_level",
        required=False,
        default='warning',
        help="log level, default warning",
        type=str
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    check_p2p_access()
    infinity.check_infinity_supported()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    #16 GB pre allocated
    #TODO: find the minimum size for pinning memory and ib_reg_mr
    infinity.register_server(loop, 16<<30)
    config = uvicorn.Config(
    app,
    host=args.host,
    port=args.port,
    loop="uvloop",
    log_level=args.log_level  # Disables logging
    )

    server = uvicorn.Server(config)

    # 运行服务器
    loop.run_until_complete(server.serve())
