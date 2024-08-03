from volga_rust import Channel, LocalChannel, RemoteChannel, RustDataReader, RustDataWriter, RustIOLoop
import msgpack
import time
import threading

lc = LocalChannel('ch_0', 'ipc:///tmp/ipc_0')
now = int(time.time())
job_name = f"job-{now}"

data_reader = RustDataReader('test_reader', job_name, [lc])
data_writer = RustDataWriter('test_writer', job_name, [lc])
io_loop = RustIOLoop()
io_loop.register_data_writer(data_writer)
io_loop.register_data_reader(data_reader)
data_writer.start()
data_reader.start()
io_loop.start(1)

num_msgs = 10000000
msg_size = 1024
batch_size = 1000

msgs = ['a' * msg_size] * num_msgs

def send():
    batch = []
    for msg in msgs:
        batch.append(msg)
        if len(batch) == batch_size:
            b = msgpack.dumps(batch)
            res = None
            while res is None:
                res = data_writer.write_bytes(lc.channel_id, b, 1000, 1)
            batch = []
    if len(batch) != 0:
        b = msgpack.dumps(batch)
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
    _batch = msgpack.loads(_b)
    rcvd.extend(_batch)

run_ts = time.time() - start_ts
io_loop.close()
data_reader.close()
data_writer.close()
assert msgs == rcvd
throughput = num_msgs/run_ts
print(f'Assert ok, finished in {run_ts}s, throughput: {throughput} msg/sec')
