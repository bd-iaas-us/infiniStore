import torch
from infinity import lib
import pytest



def test_with_statement():
    l = []
    for i in range(1000, 0, -1):
        l.append(i)
    kvcache = torch.tensor(l, device="cuda", dtype=torch.float32)
    #read "example_key" from infinity into the first 50 elements
    with lib.InfinityConnection() as conn:
        conn.write_kvcache(kvcache, "test", 950, 50)
        conn.read_kvcache(kvcache, "test", 0, 50)
    assert torch.equal(kvcache[:50], kvcache[950:1000])


#TODO: test more data types
@pytest.mark.parametrize("dtype", [torch.float16, torch.float32])
def test_read_write_kvcache(dtype):
    conn = lib.InfinityConnection()
    conn.connect()
    key = "ABCD"
    # l = []
    # a
    # for i in range(0, 1000):
    #     l.append(i)
    #kvcache = torch.tensor(l, device="cuda", dtype=dtype)
    kvcache = torch.rand(1000, device="cuda", dtype=dtype)

    #write the last 50 elements
    conn.write_kvcache(kvcache, key, 950, 50)


    #read the last 50 elements
    conn.read_kvcache(kvcache, key, 0, 50)


    #read "example_key" from infinity into the first 50 elements
    assert torch.equal(kvcache[:50], kvcache[950:1000])
    conn.close_connection()



    