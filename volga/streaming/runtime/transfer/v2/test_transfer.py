import time
import unittest
from threading import Thread
from typing import List, Dict, Any

import ray
import zmq.asyncio as zmq_async

from volga.streaming.runtime.transfer.channel import Channel, LocalChannel
from volga.streaming.runtime.transfer.v2.data_reader import DataReaderV2
from volga.streaming.runtime.transfer.v2.data_writer import DataWriterV2


TERMINAL_MESSAGE = {'data': 'done'}

@ray.remote
class Writer:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_writer = DataWriterV2(
            name='test_writer',
            source_stream_name='0',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq_async.Context.instance()
        )
        print('writer inited')

    def send_items(self, items: List[Dict]):
        self.data_writer.start()
        for item in items:
            self.data_writer._write_message(self.channel.channel_id, item)
            time.sleep(0.1)
        self.data_writer._write_message(self.channel.channel_id, TERMINAL_MESSAGE)


@ray.remote
class Reader:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_reader = DataReaderV2(
            name='test_reader',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq_async.Context.instance()
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
            ipc_addr_to='ipc:///tmp/zmqtest_to',
            ipc_addr_from='ipc:///tmp/zmqtest_from'
        )
        ray.init()
        num_items = 100
        items = [{
            'data': f'{i}'
        } for i in range(num_items)]

        reader = Reader.remote(channel)
        writer = Writer.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(items)
        received = ray.get(reader.receive_items.remote())

        assert items == received

        ray.shutdown()

    def test_one_to_one_local(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr_to='ipc:///tmp/zmqtest_to',
            ipc_addr_from='ipc:///tmp/zmqtest_from'
        )
        node_id = '0'
        zmq_ctx = zmq_async.Context.instance()
        data_reader = DataReaderV2(name='test_reader', channels=[channel], node_id=node_id, zmq_ctx=zmq_ctx)
        data_writer = DataWriterV2(name='test_writer', source_stream_name='0', channels=[channel], node_id=node_id, zmq_ctx=zmq_ctx)
        data_reader.start()
        data_writer.start()

        def write():
            for i in range(10):
                msg = {'i': i}
                data_writer._write_message(channel.channel_id, msg)
                print(f'Write: {msg}')
                time.sleep(0.1)

        wt = Thread(target=write)

        def read():
            while True:
                msg = data_reader.read_message()
                if msg is None:
                    time.sleep(0.01)
                    continue
                print(f'Read: {msg}')

        rt = Thread(target=read)

        wt.start()
        rt.start()
        time.sleep(2)

        data_writer.close()
        data_reader.close()


if __name__ == '__main__':
    t = TestLocalTransferV2()
    t.test_one_to_one_local()
    # t.test_async_zmq()