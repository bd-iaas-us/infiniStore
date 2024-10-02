from infinity import _infinity

import torch
import os
from typing import List, Tuple


def register_server(loop):
    #client does not need to call this function
    from uvloop.loop import libuv_get_loop_t_ptr
    import ctypes
    from ctypes import pythonapi, c_void_p, py_object
    PyCapsule_GetPointer = pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = c_void_p
    PyCapsule_GetPointer.argtypes = [py_object, ctypes.c_char_p]
    loop_ptr = PyCapsule_GetPointer(libuv_get_loop_t_ptr(loop), None)

    #from cpython.pycapsule import PyCapsule_GetPointer
    #<uint64_t>PyCapsule_GetPointer(obj, NULL)
    return _infinity.register_server(loop_ptr)

class DisableTorchCaching:
    def __enter__(self):
        os.environ['PYTORCH_NO_CUDA_MEMORY_CACHING'] = '1'
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        del os.environ['PYTORCH_NO_CUDA_MEMORY_CACHING']
        return
    
class InfinityConnection:
    OP_R="R"
    OP_W="W"
    OP_SYNC="S"
    OP_RDMA_WRITE="D"
    OP_RDMA_READ="A"

    def __init__(self, ):
        self.conn = _infinity.Connection()
        self.local_connected = False
        self.rdma_connected = False
    
    def connect(self, ip_addr : str):
        if self.local_connected:
            raise Exception("Already connected to local instance")
        if self.rdma_connected:
            raise Exception("Already connected to remote instance")
        ret = _infinity.init_connection(self.conn, ip_addr)
        if ret < 0:
            raise Exception("Failed to initialize remote connection")
        ret = _infinity.setup_rdma(self.conn)
        if ret < 0:
            raise Exception("Failed to setup RDMA connection")
        self.rdma_connected = True
        
    def local_connect(self):
        if self.rdma_connected:
            raise Exception("Already connected to rdma instance")     
        if self.local_connected:
            raise Exception("Already connected to local instance")
        ret = _infinity.init_connection(self.conn, "127.0.0.1")
        if ret < 0:
            raise Exception("Failed to initialize local connection")
        self.local_connected = True

    def write_kvcache(self, kvcache : torch.Tensor, blocks: List[Tuple[str, int]], page_size: int):
        self._verify(kvcache)
        ptr = kvcache.data_ptr()
        element_size = kvcache.element_size()
        #each offset should multiply by the element size
        blocks_in_bytes = [(key, offset * element_size) for key, offset in blocks]
        if self.local_connected:
            ret = _infinity.rw_local(self.conn, self.OP_W, blocks_in_bytes, page_size * element_size, ptr)
            if ret < 0:
                raise Exception(f"Failed to write to infinity, ret = {ret}")
        elif self.rdma_connected:
            ret = _infinity.rw_rdma(self.conn, self.OP_RDMA_WRITE, blocks_in_bytes, page_size * element_size, ptr)
            if ret < 0:
                raise Exception(f"Failed to write to infinity, ret = {ret}")
        else:
            raise Exception("Not connected to any instance")

    def read_kvcache(self, kvcache : torch.Tensor, blocks: List[Tuple[str, int]], page_size: int):
        self._verify(kvcache)
        ptr = kvcache.data_ptr()
        element_size = kvcache.element_size()
        #each offset should multiply by the element size
        blocks_in_bytes = [(key, offset * element_size) for key, offset in blocks]
        if self.local_connected:
            ret = _infinity.rw_local(self.conn, self.OP_R, blocks_in_bytes, page_size * element_size, ptr)
            if ret < 0:
                raise Exception(f"Failed to read to infinity, ret = {ret}")
        elif self.rdma_connected:
            ret = _infinity.rw_rdma(self.conn, self.OP_RDMA_READ, blocks_in_bytes, page_size * element_size, ptr)
            if ret < 0:
                raise Exception(f"Failed to read to infinity, ret = {ret}")
        else:
            raise Exception("Not connected to any instance")
    
    def sync(self):
        if self.local_connected:
            return _infinity.sync_local(self.conn)
        elif self.rdma_connected:
            _infinity.sync_rdma(self.conn)
        else:
            raise Exception("Not connected to any instance")
        return


    def _verify(self, kv_cache : torch.Tensor):
        if kv_cache.device.type != "cuda":
            raise Exception("Tensor must be on CUDA device")
        if kv_cache.is_contiguous() is False:
            raise Exception("Tensor must be contiguous")

