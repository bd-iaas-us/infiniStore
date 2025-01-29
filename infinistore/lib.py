import infinistore._infinistore as _infinistore

# sphinx-doc will mock infinistore._infinistore, it has to be written like this

import torch
import os
from typing import List, Tuple
import subprocess
import time

# connection type: default is RDMA
TYPE_LOCAL_GPU = "LOCAL_GPU"
TYPE_RDMA = "RDMA"
# rdma link type
LINK_ETHERNET = "Ethernet"
LINK_IB = "IB"


class ClientConfig(_infinistore.ClientConfig):
    """
    ClientConfig is a configuration class for the Infinistore client.

    Attributes:
        connection_type (str): The type of connection to use (e.g., TYPE_LOCAL_GPU, TYPE_RDMA).
        host_addr (str): The address of the host.
        dev_name (str): The name of the device (default is "mlx5_1").
        ib_port (int): The port number of the InfiniBand device (default is 1).
        link_type (str): The type of link (default is "IB").
        service_port (int): The port number of the service.
        log_level (str): The logging level (default is "warning").
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.connection_type = kwargs.get("connection_type", None)
        self.host_addr = kwargs.get("host_addr", None)
        self.dev_name = kwargs.get("dev_name", "mlx5_1")
        self.ib_port = kwargs.get("ib_port", 1)
        self.link_type = kwargs.get("link_type", "IB")
        self.service_port = kwargs.get("service_port", None)
        # get log from system env
        # if log level is not set in Config and system env is not set either, use warning as default
        if "INFINISTORE_LOG_LEVEL" in os.environ:
            self.log_level = os.environ["INFINISTORE_LOG_LEVEL"]
        else:
            self.log_level = kwargs.get("log_level", "warning")

    def __repr__(self):
        return (
            f"ServerConfig(service_port={self.service_port}, "
            f"log_level='{self.log_level}', host_addr='{self.host_addr}', "
            f"connection_type='{self.connection_type.name}')"
            f"dev_name='{self.dev_name}', ib_port={self.ib_port}, link_type='{self.link_type}'"
        )

    def verify(self):
        if self.connection_type not in [TYPE_LOCAL_GPU, TYPE_RDMA]:
            raise Exception("Invalid connection type")
        if self.host_addr == "":
            raise Exception("Host address is empty")
        if self.service_port == 0:
            raise Exception("Service port is 0")
        if self.log_level not in ["error", "debug", "info", "warning"]:
            raise Exception("log level should be error, debug, info or warning")
        if self.ib_port < 1:
            raise Exception("ib port of device should be greater than 0")
        if self.connection_type == TYPE_RDMA and self.link_type not in [
            "IB",
            "Ethernet",
        ]:
            raise Exception("link type should be IB or Ethernet for RDMA connection")


class ServerConfig(_infinistore.ServerConfig):
    class ServerConfig:
        """
        ServerConfig is a configuration class for the server settings.

        Attributes:
            manage_port (int): The port used for management. Defaults to 0.
            service_port (int): The port used for service. Defaults to 0.
            log_level (str): The logging level. Defaults to "warning".
            dev_name (str): The device name. Defaults to "mlx5_1".
            ib_port (int): The InfiniBand port number. Defaults to 1.
            link_type (str): The type of link. Defaults to "IB".
            prealloc_size (int): The preallocation size. Defaults to 16.
            minimal_allocate_size (int): The minimal allocation size. Defaults to 64.
            num_stream (int): The number of streams. Defaults to 1.
            auto_increase (bool): indicate if infinistore will be automatically increased. 10GB each time. Default False.
        """

    def __init__(self, **kwargs):
        super().__init__()
        self.manage_port = kwargs.get("manage_port", 0)
        self.service_port = kwargs.get("service_port", 0)
        self.log_level = kwargs.get("log_level", "warning")
        self.dev_name = kwargs.get("dev_name", "mlx5_1")
        self.ib_port = kwargs.get("ib_port", 1)
        self.link_type = kwargs.get("link_type", "IB")
        self.prealloc_size = kwargs.get("prealloc_size", 16)
        self.minimal_allocate_size = kwargs.get("minimal_allocate_size", 64)
        self.num_stream = kwargs.get("num_stream", 1)
        self.auto_increase = kwargs.get("auto_increase", False)

    def __repr__(self):
        return (
            f"ServerConfig: service_port={self.service_port}, manage_port={self.manage_port}, "
            f"log_level='{self.log_level}', "
            f"dev_name='{self.dev_name}', ib_port={self.ib_port}, link_type='{self.link_type}', "
            f"prealloc_size={self.prealloc_size}, minimal_allocate_size={self.minimal_allocate_size}, "
            f"num_stream={self.num_stream}"
        )

    def verify(self):
        if self.service_port == 0:
            raise Exception("Service port is 0")
        if self.manage_port == 0:
            raise Exception("Manage port is 0")
        if self.log_level not in ["error", "debug", "info", "warning"]:
            raise Exception("log level should be error, debug, info or warning")
        if self.ib_port < 1:
            raise Exception("ib port of device should be greater than 0")
        if self.link_type not in ["IB", "Ethernet"]:
            raise Exception("link type should be IB or Ethernet")
        if self.minimal_allocate_size < 16:
            raise Exception("minimal allocate size should be greater than 16")


class Logger:
    @staticmethod
    def info(msg):
        _infinistore.log_msg("info", str(msg))

    @staticmethod
    def debug(msg):
        _infinistore.log_msg("debug", str(msg))

    @staticmethod
    def error(msg):
        _infinistore.log_msg("error", str(msg))

    @staticmethod
    def warn(msg):
        _infinistore.log_msg("warning", str(msg))

    @staticmethod
    def set_log_level(level):
        _infinistore.set_log_level(level)


def register_server(loop, config: ServerConfig):
    """
    Registers a server with the given event loop and configuration.

    This function is intended to be used internally and should not be called by clients directly.

    Args:
        loop: The event loop to register the server with.
        config (ServerConfig): The configuration for the server.

    Raises:
        Exception: If the server registration fails.
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
    if _infinistore.register_server(loop_ptr, config) < 0:
        raise Exception("Failed to register server")


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


def check_supported():
    # check if kernel module nv_peer_mem is available
    if (
        "nv_peer_mem" not in _kernel_modules()
        and "nvidia_peermem" not in _kernel_modules()  # noqa: W503
    ):
        Logger.warn("nv_peer_mem or nvidia_peermem module is not loaded")
    _check_rdma_devices_ibv()


class DisableTorchCaching:
    """
    Context manager to disable PyTorch CUDA memory caching.

    When this context manager is entered, it sets the environment variable
    "PYTORCH_NO_CUDA_MEMORY_CACHING" to "1", which disables CUDA memory caching
    in PyTorch. When the context manager is exited, the environment variable is
    deleted, restoring the default behavior.

    Usage:
        with DisableTorchCaching():
            # Your code here
    """

    def __enter__(self):
        os.environ["PYTORCH_NO_CUDA_MEMORY_CACHING"] = "1"
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del os.environ["PYTORCH_NO_CUDA_MEMORY_CACHING"]
        return


class InfinityConnection:
    """
    A class to manage connections and data transfers with an Infinistore instance using either local or RDMA connections.

    Attributes:
        conn (_infinistore.Connection): The connection object to the Infinistore instance.
        local_connected (bool): Indicates if connected to a local instance.
        rdma_connected (bool): Indicates if connected to a remote instance via RDMA.
        config (ClientConfig): Configuration object for the connection.
    """

    OP_R = "R"
    OP_W = "W"
    OP_SYNC = "S"
    OP_RDMA_READ = "A"

    def __init__(self, config: ClientConfig):
        config.verify()
        self.conn = _infinistore.Connection()
        self.local_connected = False
        self.rdma_connected = False
        self.config = config
        Logger.set_log_level(config.log_level)

    def connect(self):
        """
        Establishes a connection to the Infinistore instance based on the configuration.

        Raises:
            Exception: If already connected to a local instance.
            Exception: If already connected to a remote instance.
            Exception: If failed to initialize remote connection.
            Exception: If local GPU connection is not to localhost.
            Exception: If failed to setup RDMA connection.
        """
        if self.local_connected:
            raise Exception("Already connected to local instance")
        if self.rdma_connected:
            raise Exception("Already connected to remote instance")
        ret = _infinistore.init_connection(self.conn, self.config)
        if ret < 0:
            raise Exception("Failed to initialize remote connection")

        if self.config.connection_type == TYPE_LOCAL_GPU:
            if self.config.host_addr not in ["127.0.0.1", "localhost"]:
                raise Exception("Local GPU connection must be to localhost")
            self.local_connected = True
        else:
            ret = _infinistore.setup_rdma(self.conn, self.config)
            if ret < 0:
                raise Exception("Failed to setup RDMA connection")
            self.rdma_connected = True

    def local_gpu_write_cache(
        self, cache: torch.Tensor, blocks: List[Tuple[str, int]], page_size: int
    ):
        """
        Writes a tensor to the local GPU cache.
        Args:
            cache (torch.Tensor): The tensor to be written to the cache.
            blocks (List[Tuple[str, int]]): A list of tuples where each tuple contains a key and an offset.
            page_size (int): The size of each page in the cache.
        Raises:
            Exception: If writing to infinistore fails.
        Returns:
            int: Returns 0 on success.
        """

        self._verify(cache)
        ptr = cache.data_ptr()
        element_size = cache.element_size()
        assert self.local_connected
        blocks_in_bytes = [(key, offset * element_size) for key, offset in blocks]
        device_id = cache.device.index
        cuda_visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        if len(cuda_visible_devices) > 0:
            device_id = int(cuda_visible_devices.split(",")[cache.device.index])
        ret = _infinistore.rw_local(
            self.conn,
            self.OP_W,
            blocks_in_bytes,
            page_size * element_size,
            ptr,
            device_id,
        )
        if ret < 0:
            raise Exception(f"Failed to write to infinistore, ret = {ret}")
        return 0

    def rdma_write_cache(
        self, cache: torch.Tensor, offsets: List[int], page_size, remote_blocks: List
    ):
        """
        Writes the given cache tensor to remote memory using RDMA (Remote Direct Memory Access).

        Args:
            cache (torch.Tensor): The tensor containing the data to be written to remote memory.
            offsets (List[int]): A list of offsets (in elements) where the data should be written.
            page_size (int): The size of each page to be written, in elements.
            remote_blocks (List): A list of remote memory blocks where the data should be written.

        Raises:
            AssertionError: If RDMA is not connected.
            Exception: If the RDMA write operation fails.

        Returns:
            int: Returns 0 on success.
        """

        assert self.rdma_connected
        self._verify(cache)
        ptr = cache.data_ptr()
        element_size = cache.element_size()

        # each offset should multiply by the element size
        offsets_in_bytes = [offset * element_size for offset in offsets]
        ret = _infinistore.w_rdma(
            self.conn,
            offsets_in_bytes,
            page_size * element_size,
            remote_blocks,
            ptr,
        )
        if ret < 0:
            raise Exception(f"Failed to write to infinistore, ret = {ret}")
        return 0

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
        device_id = cache.device.index
        cuda_visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        if len(cuda_visible_devices) > 0:
            device_id = int(cuda_visible_devices.split(",")[cache.device.index])
        if self.local_connected:
            ret = _infinistore.rw_local(
                self.conn,
                self.OP_R,
                blocks_in_bytes,
                page_size * element_size,
                ptr,
                device_id,
            )
            if ret < 0:
                raise Exception(f"Failed to read to infinistore, ret = {ret}")
        elif self.rdma_connected:
            ret = _infinistore.r_rdma(
                self.conn,
                blocks_in_bytes,
                page_size * element_size,
                ptr,
            )
            if ret < 0:
                raise Exception(f"Failed to read to infinistore, ret = {ret}")
        else:
            raise Exception("Not connected to any instance")

    def sync(self):
        """
        Synchronizes the current instance with the connected infinistore instance.
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
                ret = _infinistore.sync_local(self.conn)
                if ret < 0:
                    raise Exception(f"Failed to sync to infinistore, ret = {ret}")
                elif ret > 0:
                    # how many inflight requests
                    # print(f"waiting for {ret} inflight requests")
                    if n > timeout * 10000:
                        raise Exception("Timeout waiting for inflight requests")
                    time.sleep(ret * 0.0005)
                else:
                    return
        elif self.rdma_connected:
            ret = _infinistore.sync_rdma(self.conn)
        else:
            raise Exception("Not connected to any instance")

        if ret < 0:
            raise Exception(f"Failed to sync to infinistore, ret = {ret}")
        return

    def _verify(self, cache: torch.Tensor):
        if (not self.rdma_connected) and cache.device.type != "cuda":
            raise Exception("Tensor must be on CUDA device for local GPU connection")
        if cache.is_contiguous() is False:
            raise Exception("Tensor must be contiguous")

    def check_exist(self, key: str):
        """
        Check if a given key exists in the store.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the key exists, False otherwise.

        Raises:
            Exception: If there is an error checking the key's existence.
        """
        ret = _infinistore.check_exist(self.conn, key)
        if ret < 0:
            raise Exception("Failed to check if this key exists")
        return True if ret == 0 else False

    def get_match_last_index(self, keys: List[str]):
        """
        Retrieve the last index of a match for the given keys.

        Args:
            keys (List[str]): A list of string keys to search for matches.

        Returns:
            int: The last index of a match.

        Raises:
            Exception: If no match is found (i.e., if the return value is negative).
        """
        ret = _infinistore.get_match_last_index(self.conn, keys)
        if ret < 0:
            raise Exception("can't find a match")
        return ret

    def register_mr(self, cache: torch.Tensor):
        """
        Registers a memory region for RDMA (Remote Direct Memory Access) operations.

        Args:
            cache (torch.Tensor): The tensor whose memory region is to be registered.

        Returns:
            int: A positive integer indicating the registration was successful.

        Raises:
            Exception: If RDMA is not connected or if the memory region registration fails.
        """
        self._verify(cache)
        ptr = cache.data_ptr()
        element_size = cache.element_size()
        if not self.rdma_connected:
            raise Exception("this function is only valid for connected rdma")
        ret = _infinistore.register_mr(self.conn, ptr, cache.numel() * element_size)
        if ret < 0:
            raise Exception("register memory region failed")
        return ret

    def allocate_rdma(self, keys: List[str], page_size_in_bytes: int):
        """
        Allocates RDMA memory for the given keys. For RDMA writes, user must first allocate RDMA memory.
        and then use the allocated RDMA memory address to write data to the remote memory.

        Args:
            keys (List[str]): A list of keys for which RDMA memory is to be allocated.
            page_size_in_bytes (int): The size of each page in bytes.

        Returns:
            List: A list of allocated RDMA memory addresses.

        Raises:
            Exception: If RDMA is not connected.
            Exception: If memory allocation fails.
        """
        if not self.rdma_connected:
            raise Exception("this function is only valid for connected rdma")
        ret = _infinistore.allocate_rdma(self.conn, keys, page_size_in_bytes)
        if len(ret) == 0:
            raise Exception("allocate memory failed")
        return ret
