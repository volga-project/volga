import time
import unittest
from threading import Thread
from typing import List, Dict, Any

import ray
import zmq

from volga.streaming.runtime.network.channel import Channel, LocalChannel
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter


@ray.remote
class Writer:
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
        # self.data_writer.start()
        for item in items:
            self.data_writer._write_message(self.channel.channel_id, item)

    def get_stats(self):
        return self.data_writer.stats

    def close(self):
        self.io_loop.close()


@ray.remote
class Reader:
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
        # self.data_reader.start()
        t = time.time()
        res = []
        while True:
            if time.time() - t > 10:
                raise RuntimeError('Timeout reading data')

            item = self.data_reader.read_message()
            if item is None:
                time.sleep(0.001)
                continue
            print(f'rcvd_{item}')
            res.append(item)
            if len(res) == self.num_expected:
                break
        return res

    def get_stats(self):
        return self.data_reader.stats

    def close(self):
        self.io_loop.close()


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_ray(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init()
        num_items = 1000
        items = [{
            'data': f'{i}'
        } for i in range(num_items)]

        reader = Reader.remote(channel, num_items)
        writer = Writer.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(items)
        res = ray.get(reader.receive_items.remote())
        assert len(res) == num_items
        time.sleep(1)
        reader_stats: DataReader._Stats = ray.get(reader.get_stats.remote())
        writer_stats: DataWriter._Stats = ray.get(writer.get_stats.remote())

        assert reader_stats.msgs_rcvd[channel.channel_id] == num_items
        assert reader_stats.acks_sent[channel.channel_id] == num_items
        assert writer_stats.acks_rcvd[channel.channel_id] == num_items
        assert writer_stats.msgs_sent[channel.channel_id] == num_items

        print('assert ok')

        ray.get(reader.close.remote())
        ray.get(reader.close.remote())

        time.sleep(1)

        ray.shutdown()

    def test_one_to_one_local(self):
        num_items = 1000
        io_loop = IOLoop()
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(name='test_writer', source_stream_name='0', channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        data_reader = DataReader(name='test_reader', channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        io_loop.start()

        def write():
            # data_writer.start()
            for i in range(num_items):
                msg = {'i': i}
                data_writer._write_message(channel.channel_id, msg)

        num_rcvd = [0]

        def read():
            # data_reader.start()
            t = time.time()
            while True:
                if time.time() - t > 10:
                    raise RuntimeError('Timeout reading data')
                msg = data_reader.read_message()
                if msg is None:
                    time.sleep(0.001)
                    continue
                else:
                    num_rcvd[0] += 1
                if num_rcvd[0] == num_items:
                    break
        wt = Thread(target=write)

        wt.start()
        read()
        time.sleep(1)

        reader_stats = data_reader.stats
        writer_stats = data_writer.stats

        assert num_items == num_rcvd[0]
        assert reader_stats.msgs_rcvd[channel.channel_id] == num_items
        assert reader_stats.acks_sent[channel.channel_id] == num_items
        assert writer_stats.acks_rcvd[channel.channel_id] == num_items
        assert writer_stats.msgs_sent[channel.channel_id] == num_items

        print('assert ok')
        # data_writer.close()
        # data_reader.close()
        io_loop.close()
        wt.join(5)


if __name__ == '__main__':
    t = TestLocalTransfer()
    # t.test_one_to_one_local()
    t.test_one_to_one_ray()

