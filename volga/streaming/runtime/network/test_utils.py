import time
from typing import List, Any, Dict

import zmq

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
import ray

import volga

REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV = {
    'pip': [
        'pydantic==1.10.13',
        'simplejson==3.19.2',
        'pyzmq==23.2.0',
        'shared-memory-dict==0.7.2',
        'lockit==1.0.1',
        'aenum'
    ],
    'py_modules': [volga]
}

@ray.remote
class TestWriter:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.io_loop = IOLoop()
        self.data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        self.io_loop.register(self.data_writer)
        self.io_loop.start()
        print('writer inited')

    def send_items(self, items: List[Dict]):
        for item in items:
            self.data_writer._write_message(self.channel.channel_id, item)

    def get_stats(self):
        return self.data_writer.stats

    def close(self):
        self.io_loop.close()


@ray.remote
class TestReader:
    def __init__(
        self,
        channel: Channel,
        num_expected: int,
    ):
        self.channel = channel
        self.io_loop = IOLoop()
        self.data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        self.num_expected = num_expected
        self.io_loop.register(self.data_reader)
        self.io_loop.start()
        print('reader inited')

    def receive_items(self) -> List[Any]:
        t = time.time()
        res = []
        while True:
            if time.time() - t > 10:
                raise RuntimeError('Timeout reading data')

            item = self.data_reader.read_message()
            if item is None:
                time.sleep(0.001)
                continue
            res.append(item)
            if len(res) == self.num_expected:
                break
        return res

    def get_stats(self):
        return self.data_reader.stats

    def close(self):
        self.io_loop.close()


# local utils
def read(rcvd: List, data_reader: DataReader, num_expected: int):
    t = time.time()
    while True:
        if time.time() - t > 10:
            raise RuntimeError('Timeout reading data')
        msg = data_reader.read_message()
        if msg is None:
            time.sleep(0.001)
            continue
        else:
            rcvd.append(msg)
        if len(rcvd) == num_expected:
            break


def write(to_send: List, data_writer: DataWriter, channel: Channel):
    for msg in to_send:
        data_writer._write_message(channel.channel_id, msg)
