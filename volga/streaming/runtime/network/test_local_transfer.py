import functools
import time
import unittest
from threading import Thread
from typing import List, Dict, Any

import ray
import zmq

from volga.streaming.runtime.network.channel import Channel, LocalChannel
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.test_utils import TestReader, TestWriter, write, read
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init()
        num_items = 1000
        to_send = [{'i': i} for i in range(num_items)]

        reader = TestReader.remote(channel, num_items)
        writer = TestWriter.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(to_send)
        rcvd = ray.get(reader.receive_items.remote())
        time.sleep(1)

        assert to_send == sorted(rcvd, key=lambda e: e['i'])
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
        rcvd = []

        wt = Thread(target=functools.partial(write, to_send, data_writer, channel))
        wt.start()
        read(rcvd, data_reader, num_items)
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
    t.test_one_to_one_on_ray()

