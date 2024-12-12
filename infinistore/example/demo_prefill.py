from infinistore import (
    ClientConfig,
    check_supported,
    DisableTorchCaching,
    InfinityConnection,
)
import infinistore
import torch
import time
import torch.nn as nn

import queue
import threading

N = 4096
num_layers = 14
num_heads = 8
seq_length = 5000


class TransformerLayer(nn.Module):
    def __init__(self, embed_dim, num_heads):
        super().__init__()
        self.mha = nn.MultiheadAttention(
            embed_dim, num_heads, device="cuda:0", dtype=torch.float16
        )
        self.norm = nn.LayerNorm(embed_dim, device="cuda:0", dtype=torch.float16)
        self.ffn = nn.Sequential(
            nn.Linear(embed_dim, embed_dim, device="cuda:0", dtype=torch.float16),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim, device="cuda:0", dtype=torch.float16),
        )

    def forward(self, x):
        attn_output, _ = self.mha(x, x, x)
        x = self.norm(x + attn_output)

        ffn_output = self.ffn(x)
        x = self.norm(x + ffn_output)
        return x


def run(conn):
    check_supported()
    with DisableTorchCaching():
        model = nn.Sequential(
            *[TransformerLayer(N, num_heads) for _ in range(num_layers)]
        ).cuda()

    input = torch.randn(seq_length, 1, N, device="cuda:0", dtype=torch.float16)

    torch.cuda.synchronize(0)
    now1 = time.time()

    output = input

    upload_queue = queue.Queue()

    def upload_worker():
        while True:
            layer_idx, event, data = upload_queue.get()
            event.synchronize()
            blocks = [(f"key{i}_{j}", j * 4096) for j in range(5000)]
            conn.local_gpu_write_cache(output, blocks, 4096)
            upload_queue.task_done() 

    upload_thread = threading.Thread(target=upload_worker, daemon=True)
    upload_thread.start()
    events = [torch.cuda.Event() for _ in range(len(model))]
    outputs = []

    for i, layer in enumerate(model):
        output = layer(output)
        events[i].record()
        # old approach
        # outputs.append(output)
        upload_queue.put((i, events[i], output))



    # Old approach: upload kvcache after computation

    # torch.cuda.synchronize()
    # for i, output in enumerate(outputs):
    #     # split output into 1000 blocks
    #     blocks = [(f"key{i}_{j}", j*4096) for j in range(5000)]
    #     conn.local_gpu_write_cache(output, blocks, 4096)

    conn.sync()

    print("Time taken for linear layers: ", time.time() - now1)


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

    config.connection_type = infinistore.TYPE_LOCAL_GPU
    local_conn = InfinityConnection(config)
    local_conn.connect()
    run(local_conn)
