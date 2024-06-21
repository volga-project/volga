import time
import unittest
from threading import Thread
from typing import List, Dict, Any

import ray
import zmq

from volga.streaming.runtime.network.channel import Channel, LocalChannel
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter


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

    def test_one_to_one_on_ray(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init()
        num_items = 1000
        items = [{
            'data': f'{i}'
        } for i in range(num_items)]

        reader = TestReader.remote(channel, num_items)
        writer = TestWriter.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(items)
        res = ray.get(reader.receive_items.remote())
        assert len(res) == num_items
        time.sleep(1)
        reader_stats: Stats = ray.get(reader.get_stats.remote())
        writer_stats: Stats = ray.get(writer.get_stats.remote())

        assert reader_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        assert reader_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items

        print('assert ok')

        ray.get(reader.close.remote())
        ray.get(reader.close.remote())

        time.sleep(1)

        ray.shutdown()

    def test_one_to_one_locally(self):
        num_items = 10
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

        to_send = [{'i': i} for i in range(num_items)]
        def write():
            for msg in to_send:
                data_writer._write_message(channel.channel_id, msg)

        rcvd = []
        def read():
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
                if len(rcvd) == num_items:
                    break
        wt = Thread(target=write)

        wt.start()
        read()
        time.sleep(1)

        reader_stats = data_reader.stats
        writer_stats = data_writer.stats
        assert to_send == sorted(rcvd, key=lambda e: e['i'])
        assert reader_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        assert reader_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items

        print('assert ok')
        io_loop.close()
        wt.join(5)


if __name__ == '__main__':
    t = TestLocalTransfer()
    t.test_one_to_one_locally()
    # t.test_one_to_one_on_ray()

