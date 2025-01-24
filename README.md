[![Run pre-commit checks](https://github.com/bd-iaas-us/infiniStore/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/bd-iaas-us/infiniStore/actions/workflows/pre-commit.yml)

[![Slack](https://img.shields.io/badge/Slack-Join%20Us-blue?logo=slack)](https://vllm-dev.slack.com/archives/C07VCUQLE1F)


## pre-required

* CUDA dev toolkit https://developer.nvidia.com/cuda-toolkit
* kernel moduel nv_peer_mem https://github.com/Mellanox/nv_peer_memory


## install

```
pip install infinistore
```

## run

```
infinistore
```
or
```
python -m infinistore.server
```
## build

```
apt install -y libuv1-dev
apt install -y libflatbuffers-dev
apt install -y libspdlog-dev libfmt-dev
apt install -y ibverbs-utils libibverbs-dev
apt install -y libboost-dev
pip install -e .
pip install pre-commit
pre-commit install
```
## client example

check example code ```infinistore/example/client.py```


## unit test

```
pytest infinistore/test_infinistore.py
```

## pre-commit

```
pre-commit run --all-files
```
