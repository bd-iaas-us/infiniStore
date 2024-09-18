

from infinity import _infinity

import torch
import os


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
    def __init__(self):
        self.conn = _infinity.Connection()
        self.connected = False
    
    def connect(self):
        ret = _infinity.init_connection(self.conn)
        if ret < 0:
            raise Exception("Failed to initialize connection")
        self.connected = True

    def _verify(self, kv_cache : torch.Tensor, key : str, offset : int, size : int):
        if self.connected is False:
            raise Exception("Connection is not established")
        if kv_cache.device.type != "cuda":
            raise Exception("Tensor must be on CUDA device")
        if kv_cache.is_contiguous() is False:
            raise Exception("Tensor must be contiguous")
        if offset < 0:
            raise Exception("Offset must be positive")
        if kv_cache.numel() < offset + size:
            raise Exception("Offset + size is out of bound")
    
    #size is the element size
    #offset is the offset of the element
    def write_kvcache(self, kvcache : torch.Tensor, key : str, offset: int, size: int):
        self._verify(kvcache, key, offset, size)
        ptr = kvcache.data_ptr()
        #convert to byte offset, since the function expects byte offset
        offset = offset * kvcache.element_size()
        size = size * kvcache.element_size()
        
        ret = _infinity.rw_local(self.conn, self.OP_W, key, ptr, offset, size)
        if ret < 0:
            raise Exception(f"Failed to write to infinity, ret = {ret}")
        return
    
    def sync_local(self):
        ret = _infinity.sync_local(self.conn)
        if ret < 0:
            raise Exception(f"Failed to sync, ret = {ret}")
    
    def read_kvcache(self, kvcache : torch.Tensor, key : str, offset: int, size: int):
        self._verify(kvcache, key, offset, size)
        ptr = kvcache.data_ptr()
        #convert to byte offset, since the function expects byte offset
        offset = offset * kvcache.element_size()
        size = size * kvcache.element_size()
        ret = _infinity.rw_local(self.conn, self.OP_R, key, ptr, offset, size)
        if ret < 0:
            raise Exception(f"Failed to read from infinity, ret = {ret}")
        return

    def close_connection(self):
        if self.connected:
            _infinity.close_connection(self.conn)
            self.connected = False
        return
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close_connection()
