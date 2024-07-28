from volga_rust import Channel, LocalChannel, RemoteChannel, RustDataReader, RustDataWriter, RustIOLoop
import msgpack
import time
import threading

lc = LocalChannel('ch_0', 'ipc:///tmp/ipc_0')

data_reader = RustDataReader('test_reader', [lc])
data_writer = RustDataWriter('test_writer', [lc])
io_loop = RustIOLoop()
io_loop.register_data_writer(data_writer)
io_loop.register_data_reader(data_reader)
data_reader.start()
io_loop.start(1)

num_msgs = 100000
msg_size = 32*1024

msgs = ['a' * msg_size] * num_msgs

def send():
    for msg in msgs:
        b = msgpack.dumps(msg)
        res = None
        while res is None:
            res = data_writer.write_bytes(lc.channel_id, b, 1000, 1)

wt = threading.Thread(target=send)

start_ts = time.time()
wt.start()

rcvd = []

while len(rcvd) != num_msgs:
    _b = None
    while _b is None:
        _b = data_reader.read_bytes()
    _msg = msgpack.loads(_b)
    rcvd.append(_msg)
run_ts = time.time() - start_ts
io_loop.close()
data_reader.close()
assert msgs == rcvd
print(f'Assert ok, finished in {run_ts}s')
