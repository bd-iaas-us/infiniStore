import torch
import infinistore
import time
import argparse


def warm_up(args):
    # create tensor on each device, and write/read data to/from it
    num_devices = torch.cuda.device_count()
    src_tensors = []
    dst_tensors = []
    with infinistore.DisableTorchCaching():
        for i in range(num_devices):
            infinistore.Logger.info(f"detect device GPU:{i} ...")
            src_tensors.append(
                torch.randn(4096, device=f"cuda:{i}", dtype=torch.float32)
            )
            dst_tensors.append(
                torch.zeros(4096, device=f"cuda:{i}", dtype=torch.float32)
            )
            torch.cuda.synchronize(dst_tensors[i].device)

    local_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=args.service_port,
        connection_type=infinistore.TYPE_LOCAL_GPU,
        log_level="error",
    )

    # wait for server to be ready in script
    time.sleep(args.start_delay)

    conn = infinistore.InfinityConnection(local_config)
    conn.connect()
    for i in range(num_devices):
        infinistore.Logger.info(f"warming up device GPU:{i}")
        conn.local_gpu_write_cache(src_tensors[i], [(f"warmup_{i}", 0)], 4096)
        conn.sync()
        conn.read_cache(dst_tensors[i], [(f"warmup_{i}", 0)], 4096)
        conn.sync()
        # check data
        assert torch.allclose(
            src_tensors[i], dst_tensors[i]
        ), f"device {i} data mismatch"

    # TODO: delete remote keys
    src_tensors = None
    dst_tensors = None
    infinistore.Logger.info("warm up finished")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service-port",
        required=True,
        help="which port to connect to",
        type=int,
    )

    parser.add_argument(
        "--start-delay",
        required=False,
        help="delay before starting warm up, used in script",
        type=int,
        default=0,
    )
    warm_up(parser.parse_args())


if __name__ == "__main__":
    main()
