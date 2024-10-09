from infinity import _infinity

import torch
import os
from typing import List, Tuple
import subprocess
import time


def register_server(loop, config):
    """
    Registers a server with the given event loop.

    This function is intended for internal use only and should not be called by clients.

    Args:
        loop: The event loop to register the server with.

    """
    # client does not need to call this function
    from uvloop.loop import libuv_get_loop_t_ptr
    import ctypes
    from ctypes import pythonapi, c_void_p, py_object

    PyCapsule_GetPointer = pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = c_void_p
    PyCapsule_GetPointer.argtypes = [py_object, ctypes.c_char_p]
    loop_ptr = PyCapsule_GetPointer(libuv_get_loop_t_ptr(loop), None)

    # from cpython.pycapsule import PyCapsule_GetPointer
    # <uint64_t>PyCapsule_GetPointer(obj, NULL)
    return _infinity.register_server(loop_ptr, config)


def _kernel_modules():
    modules = set()
    try:
        with open("/proc/modules", "r") as f:
            for line in f:
                sep = line.find(" ")
                if sep != -1:
                    modules.add(line[:sep])
    except IOError as e:
        raise Exception(f"can not read /proc/modules: {e}")
    return modules


def _check_rdma_devices_ibv():
    try:
        result = subprocess.run(
            ["ibv_devinfo"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            return
        output = result.stdout
        devices = output.split("\n\n")
        port_active = False
        for device_info in devices:
            if "hca_id" in device_info:
                if "PORT_ACTIVE" in device_info:
                    port_active = True
                    break
        if port_active is False:
            raise Exception("No active RDMA device found")
    except FileNotFoundError:
        raise Exception(
            "command ibv_devinfo not found, make sure RDMA tools are installed; for ubuntu, run apt install ibv_devinfo"
        )


def check_infinity_supported():
    # check if kernel module nv_peer_mem is available
    if "nv_peer_mem" not in _kernel_modules():
        raise Exception("nv_peer_mem module is not loaded")
    _check_rdma_devices_ibv()


class DisableTorchCaching:
    def __enter__(self):
        os.environ["PYTORCH_NO_CUDA_MEMORY_CACHING"] = "1"
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del os.environ["PYTORCH_NO_CUDA_MEMORY_CACHING"]
        return


class InfinityConnection:
    OP_R = "R"
    OP_W = "W"
    OP_SYNC = "S"
    OP_RDMA_WRITE = "D"
    OP_RDMA_READ = "A"

    def __init__(self, config):
        self.conn = _infinity.Connection()
        self.local_connected = False
        self.rdma_connected = False
        self.config = config

    def connect(self):
        """
        Establishes an RDMA connection using the provided IP address.
        """
        if self.local_connected:
            raise Exception("Already connected to local instance")
        if self.rdma_connected:
            raise Exception("Already connected to remote instance")
        ret = _infinity.init_connection(self.conn, self.config)
        if ret < 0:
            raise Exception("Failed to initialize remote connection")
        ret = _infinity.setup_rdma(self.conn)
        if ret < 0:
            raise Exception("Failed to setup RDMA connection")
        self.rdma_connected = True

    def local_connect(self):
        """
        Establishes a local connection to the instance.

        This method initializes a local connection to the instance using the IP address "127.0.0.1".
        This connection will use cudaMemcpy to transfer data between the local and remote instances.
        It raises an exception if a connection is already established either locally or via RDMA.

        """
        if self.rdma_connected:
            raise Exception("Already connected to rdma instance")
        if self.local_connected:
            raise Exception("Already connected to local instance")
        self.config.host_addr = "127.0.0.1"
        ret = _infinity.init_connection(self.conn, self.config)
        if ret < 0:
            raise Exception("Failed to initialize local connection")
        self.local_connected = True

    def write_cache(
        self, cache: torch.Tensor, blocks: List[Tuple[str, int]], page_size: int
    ):
        """
        Writes the given cache tensor to the specified blocks in memory.

        Args:
            cache (torch.Tensor): The tensor containing the data to be written.
            blocks (List[Tuple[str, int]]): A list of tuples where each tuple contains a key and an offset.
            each pair represents a page to be written to. The page is fixed size and is specified by the page_size parameter.
            page_size (int): How many element in one page.
        """
        self._verify(cache)
        ptr = cache.data_ptr()
        element_size = cache.element_size()
        # each offset should multiply by the element size
        blocks_in_bytes = [(key, offset * element_size) for key, offset in blocks]
        if self.local_connected:
            ret = _infinity.rw_local(
                self.conn, self.OP_W, blocks_in_bytes, page_size * element_size, ptr
            )
            if ret < 0:
                raise Exception(f"Failed to write to infinity, ret = {ret}")
        elif self.rdma_connected:
            ret = _infinity.rw_rdma(
                self.conn,
                self.OP_RDMA_WRITE,
                blocks_in_bytes,
                page_size * element_size,
                ptr,
            )
            if ret < 0:
                raise Exception(f"Failed to write to infinity, ret = {ret}")
        else:
            raise Exception("Not connected to any instance")

    def read_cache(
        self, cache: torch.Tensor, blocks: List[Tuple[str, int]], page_size: int
    ):
        """
        Reads data from the cache using either local or RDMA connection.

        Args:
            cache (torch.Tensor): The tensor containing the cache data.
            blocks (List[Tuple[str, int]]): A list of tuples where each tuple contains a key and an offset.
            each pair represents a page to be written to. The page is fixed size and is specified by the page_size parameter.
            page_size (int): The size of the page to read.

        Raises:
            Exception: If the read operation fails or if not connected to any instance.
        """
        self._verify(cache)
        ptr = cache.data_ptr()
        element_size = cache.element_size()
        # each offset should multiply by the element size
        blocks_in_bytes = [(key, offset * element_size) for key, offset in blocks]
        if self.local_connected:
            ret = _infinity.rw_local(
                self.conn, self.OP_R, blocks_in_bytes, page_size * element_size, ptr
            )
            if ret < 0:
                raise Exception(f"Failed to read to infinity, ret = {ret}")
        elif self.rdma_connected:
            ret = _infinity.rw_rdma(
                self.conn,
                self.OP_RDMA_READ,
                blocks_in_bytes,
                page_size * element_size,
                ptr,
            )
            if ret < 0:
                raise Exception(f"Failed to read to infinity, ret = {ret}")
        else:
            raise Exception("Not connected to any instance")

    def sync(self):
        """
        Synchronizes the current instance with the connected infinity instance.
        This method attempts to synchronize the current instance using either a local
        connection or an RDMA connection. If neither connection is available, it raises
        an exception.
        Raises:
            Exception: If not connected to any instance.
            Exception: If synchronization fails with a negative return code.
        """
        ret = 0
        if self.local_connected:
            n = 0
            timeout = 1  # 1 second timeout
            while True:
                ret = _infinity.sync_local(self.conn)
                if ret < 0:
                    raise Exception(f"Failed to sync to infinity, ret = {ret}")
                elif ret > 0:
                    # how many inflight requests
                    # print(f"waiting for {ret} inflight requests")
                    if n > timeout * 10000:
                        raise Exception("Timeout waiting for inflight requests")
                    time.sleep(ret * 0.0001)
                else:
                    return
        elif self.rdma_connected:
            ret = _infinity.sync_rdma(self.conn)
        else:
            raise Exception("Not connected to any instance")

        if ret < 0:
            raise Exception(f"Failed to sync to infinity, ret = {ret}")
        return

    def _verify(self, cache: torch.Tensor):
        if cache.device.type != "cuda":
            raise Exception("Tensor must be on CUDA device")
        if cache.is_contiguous() is False:
            raise Exception("Tensor must be contiguous")
