[![Run pre-commit checks](https://github.com/bd-iaas-us/infiniStore/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/bd-iaas-us/infiniStore/actions/workflows/pre-commit.yml)

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
pip install pre-commit
pre-commit install
```
## run

```
python start.py
```


## unit test

```
pytest infinistore/test_infinistore.py
```

##

```
pre-commit run --all-files
```
