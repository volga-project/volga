from volga_rust import Channel, LocalChannel, RemoteChannel, RustDataReader, RustDataWriter, RustIOLoop
import msgpack
import time

lc = LocalChannel('ch_0', 'ipc:///tmp/ipc_0')

data_reader = RustDataReader('test_reader', [lc])
data_writer = RustDataWriter('test_writer', [lc])
io_loop = RustIOLoop()
io_loop.register_data_writer(data_writer)
io_loop.register_data_reader(data_reader)
data_reader.start()
io_loop.start(1)
msg = 'abc'
b = msgpack.dumps(msg)
data_writer.write_bytes(lc.channel_id, b, 1000, 1)

time.sleep(1)

_b = data_reader.read_bytes()
_msg = msgpack.loads(_b)
io_loop.close()
data_reader.close()
assert msg == _msg
print('Assert ok')
