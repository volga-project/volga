import time
import unittest
from threading import Thread
from typing import List, Dict, Any

import ray
import zmq

from volga.streaming.runtime.transfer.channel import Channel, LocalChannel
from volga.streaming.runtime.transfer.data_reader import DataReader
from volga.streaming.runtime.transfer.data_writer import DataWriter

TERMINAL_MESSAGE = {'data': 'done'}

@ray.remote
class Writer:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        print('writer inited')

    def send_items(self, items: List[Dict]):
        self.data_writer.start()
        for item in items:
            self.data_writer._write_message(self.channel.channel_id, item)
            # time.sleep(0.1)
        self.data_writer._write_message(self.channel.channel_id, TERMINAL_MESSAGE)


@ray.remote
class Reader:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        print('reader inited')

    def receive_items(self) -> List[Any]:
        self.data_reader.start()
        t = time.time()
        res = []
        while True:
            if time.time() - t > 10:
                raise RuntimeError('Timeout waiting for data')

            item = self.data_reader.read_message()
            if item is None:
                time.sleep(0.001)
                continue
            if item == TERMINAL_MESSAGE:
                break
            else:
                print(f'rcvd_{item}')
                res.append(item)
        return res


class TestLocalTransferV2(unittest.TestCase):

    def test_one_to_one_ray(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init()
        num_items = 100000
        items = [{
            'data': f'{i}'
        } for i in range(num_items)]

        reader = Reader.remote(channel)
        writer = Writer.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(items)
        reader.receive_items.remote()

        time.sleep(180)

        ray.shutdown()

    def test_one_to_one_local(self):

        num_msg = 1000

        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(name='test_writer', source_stream_name='0', channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        data_reader = DataReader(name='test_reader', channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        def write():

            data_writer.start()
            for i in range(num_msg):
                msg = {'i': i}
                data_writer._write_message(channel.channel_id, msg)
                # print(f'Write: {msg}')
                # time.sleep(0.01)

        def read():
            data_reader.start()
            while True:
                msg = data_reader.read_message()
                if msg is None:
                    continue
                time.sleep(0.001)
                # print(f'Read: {msg}')
        wt = Thread(target=write)
        rt = Thread(target=read)

        wt.start()
        rt.start()
        time.sleep(5)

        print(f'Reader stats: {data_reader.stats}')
        print(f'Writer stats: {data_writer.stats}')
        # data_writer.close()
        # data_reader.close()



if __name__ == '__main__':
    t = TestLocalTransferV2()
    t.test_one_to_one_local()
    # t.test_one_to_one_ray()
    # t.test_async_zmq()

