import functools
import time
import unittest
from threading import Thread
from typing import Optional, Any

import ray
import zmq
import random

from volga.streaming.runtime.network.channel import LocalChannel
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.testing_utils import TestReader, TestWriter, write, read, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, FakeIOLoop
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter


from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import ray.util.state as ray_state


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init(address=ray_addr, runtime_env=runtime_env)
        num_items = 1000
        to_send = [{'i': i} for i in range(num_items)]

        # make sure we schedule on the same node
        all_nodes = ray.nodes()
        if len(all_nodes) >= 2:
            # skip head node
            no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))
            node = random.sample(no_head, 1)[0]
        else:
            node = all_nodes[0]
        reader = TestReader.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(channel, num_items)
        writer = TestWriter.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(channel)

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
        print(to_send)
        print(rcvd)
        assert to_send == rcvd
        assert reader_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        assert reader_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items
        assert writer_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items

        print('assert ok')
        io_loop.close()
        wt.join(5)

    def test_unreliable_connection(self):
        loss_rate = 0.1
        exception_rate = 0.05
        out_of_orderness = 0.2
        num_items = 10000
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        data_writer = DataWriter(name='test_writer', source_stream_name='0', channels=[channel], node_id='0',
                                 zmq_ctx=None)
        data_reader = DataReader(name='test_reader', channels=[channel], node_id='0', zmq_ctx=None)

        fake_io_loop = FakeIOLoop(data_reader, data_writer, loss_rate, exception_rate, out_of_orderness)
        fake_io_loop.start()

        to_send = [{'i': i} for i in range(num_items)]
        rcvd = []

        wt = Thread(target=functools.partial(write, to_send, data_writer, channel))
        wt.start()
        read(rcvd, data_reader, num_items)
        time.sleep(1)
        fake_io_loop.close()
        assert to_send == rcvd
        print('assert ok')


if __name__ == '__main__':
    t = TestLocalTransfer()
    t.test_one_to_one_locally()
    # t.test_one_to_one_on_ray()
    # t.test_one_to_one_on_ray(ray_addr='ray://127.0.0.1:12345', runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV)
    t.test_unreliable_connection()
