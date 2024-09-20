import torch
import pytest
from infinity import InfinityConnection, DisableTorchCaching


#TODO: test more data types
@pytest.mark.parametrize("dtype", [torch.float16, torch.float32])
def test_read_write_kvcache(dtype):
    conn = InfinityConnection()
    conn.connect()
    key = "ABCD"

    with DisableTorchCaching():
        kvcache = torch.rand(1000, device="cuda", dtype=dtype)

    #write the last 50 elements
    conn.write_kvcache(kvcache, [(key, 950)], 50)
    #read the last 50 elements
    conn.read_kvcache(kvcache, [(key, 0)], 50)


    #read "example_key" from infinity into the first 50 elements
    assert torch.equal(kvcache[:50], kvcache[950:1000])
    conn.close_connection()



    