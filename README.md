
## pre-required

* CUDA dev toolkit https://developer.nvidia.com/cuda-toolkit
* kernel moduel nv_peer_mem https://github.com/Mellanox/nv_peer_memory


## build

```
apt install libuv1-dev
apt install libgtest-dev
apt install libmsgpack-dev
apt install libspdlog-dev libfmt-dev
apt install ibverbs-utils libibverbs-dev
pip install -e .
```
## run

```
python start.py
```