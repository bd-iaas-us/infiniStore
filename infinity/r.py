from infinity import _infinity

conn = _infinity.Connection()
_infinity.init_connection(conn)
_infinity.setup_rdma(conn)
_infinity.close_connection(conn)