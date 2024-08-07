import random

from volga_rust import RustLocalChannel, RustRemoteChannel, RustDataReader, RustDataWriter, RustIOLoop
import msgpack
import time
import threading

lc = RustLocalChannel('ch_0', 'ipc:///tmp/ipc_test')
rc = RustRemoteChannel('ch_0', 'ipc:///tmp/ipc_test', 'source_node_ip', 'source_node_id', 'ipc:///tmp/ipc_test', 'target_node_ip', 'target_node_id', 1234)
now = int(time.time())
job_name = f"job-{now}"

data_reader = RustDataReader('test_reader', job_name, [lc])
data_writer = RustDataWriter('test_writer', job_name, [lc])
io_loop = RustIOLoop('test_loop')
io_loop.register_data_writer(data_writer)
io_loop.register_data_reader(data_reader)
data_writer.start()
data_reader.start()
io_loop.start(1)

num_msgs = 10000
msg_size = 1024
batch_size = 1

msgs = [str(random.randint(0, 9)) * msg_size] * num_msgs
msgs_b = [b'a' * msg_size] * num_msgs

def send():
    batch = []
    for msg in msgs:
        batch.append(msg)
        if len(batch) == batch_size:
            b = msgpack.dumps(batch)
            res = None
            t = time.time()
            while res is None:
                res = data_writer.write_bytes(lc.channel_id, b, True, 1000, 1)

            t = time.time() - t
            print(f'Written in {t}s')
            batch = []
    if len(batch) != 0:
        b = msgpack.dumps(batch)
        res = None
        while res is None:
            res = data_writer.write_bytes(lc.channel_id, b, True, 1000, 1)

def send_b():
    for b in msgs_b:
        res = None
        t = time.time()
        while res is None:
            res = data_writer.write_bytes(lc.channel_id, b, 1000, 1)

        t = time.time() - t
        print(f'Written in {t}s')

# wt = threading.Thread(target=send_b)
wt = threading.Thread(target=send)

def recv():
    recvd = []

    while len(recvd) != num_msgs:
        _b = None
        while _b is None:
            _b = data_reader.read_bytes()
        _batch = msgpack.loads(_b)
        recvd.extend(_batch)
    return recvd

def recv_b():
    recvd = []
    while len(recvd) != num_msgs:
        _b = None
        while _b is None:
            _b = data_reader.read_bytes()
        recvd.append(_b)
    return recvd

start_ts = time.time()
wt.start()

print(f'Started')

recvd = recv()
# recvd_b = recv_b()
run_ts = time.time() - start_ts
io_loop.close()
data_reader.close()
data_writer.close()
# assert msgs_b == recvd_b
assert msgs == recvd
throughput = num_msgs/run_ts
print(f'Assert ok, finished in {run_ts}s, throughput: {throughput} msg/sec')
