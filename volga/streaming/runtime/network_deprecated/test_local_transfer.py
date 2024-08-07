import functools
import time
import unittest
from threading import Thread
from typing import Optional, Any

import ray
import zmq
import random

from volga.streaming.runtime.network_deprecated.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network_deprecated.buffer.buffering_policy import BufferPerMessagePolicy, PeriodicPartialFlushPolicy
from volga.streaming.runtime.network_deprecated.channel import LocalChannel
from volga.streaming.runtime.network_deprecated.stats import Stats, StatsEvent
from volga.streaming.runtime.network_deprecated.testing_utils import TestReader, TestWriter, write, read, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, FakeIOLoop, start_ray_io_handler_actors
from volga.streaming.runtime.network_deprecated.transfer.io_loop import IOLoop
from volga.streaming.runtime.network_deprecated.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network_deprecated.transfer.local.data_writer import DataWriter


from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init(address=ray_addr, runtime_env=runtime_env)
        num_items = 10000
        to_send = [{'i': i} for i in range(num_items)]
        writer_delay_s = 0

        job_name = f'job-{int(time.time())}'
        buffering_config = BufferingConfig(buffer_size=32 * 1024, capacity_per_in_channel=100, capacity_per_out=100)
        buffering_policy = PeriodicPartialFlushPolicy(0.1)
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
        ).remote(job_name, channel, num_items)
        writer = TestWriter.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(job_name, channel, writer_delay_s, buffering_policy, buffering_config)

        start_ray_io_handler_actors([reader, writer])

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(to_send)
        rcvd = ray.get(reader.receive_items.remote())
        time.sleep(1)

        # assert to_send == sorted(rcvd, key=lambda e: e['i'])
        assert to_send == rcvd
        reader_stats: Stats = ray.get(reader.get_stats.remote())
        writer_stats: Stats = ray.get(writer.get_stats.remote())

        # assert reader_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        # assert reader_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        # assert writer_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items
        # assert writer_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items

        print(reader_stats)
        print(writer_stats)
        print('assert ok')

        ray.get(reader.close.remote())
        ray.get(reader.close.remote())

        time.sleep(1)

        ray.shutdown()

    def test_one_to_one_locally(self):
        num_items = 2000
        io_loop = IOLoop()
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        zmq_ctx = zmq.Context.instance(io_threads=10)
        buffering_policy = BufferPerMessagePolicy()

        job_name = f'job-{int(time.time())}'
        data_writer = DataWriter(name='test_writer', source_stream_name='0', job_name=job_name,
                                 channels=[channel], node_id='0', zmq_ctx=zmq_ctx,
                                 buffering_policy=buffering_policy)
        data_reader = DataReader(name='test_reader', job_name=job_name, channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        succ, err = io_loop.start()
        if not succ:
            raise RuntimeError(f'Unable to connect io_loop: {err}')

        to_send = [{'i': i} for i in range(num_items)]
        rcvd = []

        wt = Thread(target=functools.partial(write, to_send, data_writer, channel))
        wt.start()
        read(rcvd, data_reader, num_items)
        time.sleep(1)

        reader_stats = data_reader.stats
        writer_stats = data_writer.stats
        # print(to_send)
        # print(rcvd)
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
        buffering_config = BufferingConfig(buffer_size=32 * 1024)
        buffering_policy = PeriodicPartialFlushPolicy(0.1)
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        job_name = f'job-{int(time.time())}'
        data_writer = DataWriter(name='test_writer', source_stream_name='0', job_name=job_name,
                                 channels=[channel], node_id='0',
                                 zmq_ctx=None, buffering_config=buffering_config, buffering_policy=buffering_policy)
        data_reader = DataReader(name='test_reader', job_name=job_name, channels=[channel], node_id='0', zmq_ctx=None)

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

    def test_buffer_batching(self):
        num_items = 1000000
        buffering_config = BufferingConfig(buffer_size=132 * 1024)
        buffering_policy = PeriodicPartialFlushPolicy(0.1)
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )

        to_send = [{'i': i} for i in range(num_items)]

        io_loop = IOLoop()

        job_name = f'job-{int(time.time())}'
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(name='test_writer', source_stream_name='0', job_name=job_name,
                                 channels=[channel], node_id='0',
                                 zmq_ctx=zmq_ctx,
                                 buffering_config=buffering_config, buffering_policy=buffering_policy)
        data_reader = DataReader(name='test_reader', job_name=job_name, channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        succ, err = io_loop.start()
        if not succ:
            raise RuntimeError(f'Unable to connect io_loop: {err}')
        rcvd = []

        wt = Thread(target=functools.partial(write, to_send, data_writer, channel))
        wt.start()
        read(rcvd, data_reader, num_items)
        time.sleep(1)

        assert to_send == rcvd

        print('assert ok')
        io_loop.close()
        wt.join(5)

    def test_backpressure(self):
        buffering_config = BufferingConfig(capacity_per_in_channel=3, capacity_per_out=2)
        buffering_policy = BufferPerMessagePolicy()
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )

        io_loop = IOLoop()

        job_name = f'job-{int(time.time())}'
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(name='test_writer', source_stream_name='0', job_name=job_name, channels=[channel], node_id='0',
                                 zmq_ctx=zmq_ctx,
                                 buffering_policy=buffering_policy, buffering_config=buffering_config)
        data_reader = DataReader(name='test_reader', job_name=job_name, channels=[channel], node_id='0', zmq_ctx=zmq_ctx)
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        succ, err = io_loop.start()
        if not succ:
            raise RuntimeError(f'Unable to connect io_loop: {err}')
        try:
            for _ in range(buffering_config.capacity_per_in_channel + buffering_config.capacity_per_out):
                time.sleep(0.1)
                s = data_writer._write_message(channel_id=channel.channel_id, message={'k': 'v'}, block=False)
                assert s is True

            time.sleep(0.1)
            s = data_writer._write_message(channel_id=channel.channel_id, message={'k': 'v'}, block=False)
            # should backpressure
            assert s is False

            # read one
            time.sleep(0.1)
            data_reader.read_message()
            time.sleep(0.1)
            s = data_writer._write_message(channel_id=channel.channel_id, message={'k': 'v'}, block=False)
            # should not backpressure
            assert s is True

            # TODO test queue lengths
            print('assert ok')
        finally:
            io_loop.close()


if __name__ == '__main__':
    t = TestLocalTransfer()
    t.test_one_to_one_locally()
    t.test_one_to_one_on_ray()
    # t.test_one_to_one_on_ray(ray_addr='ray://127.0.0.1:12345', runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV)
    t.test_unreliable_connection()
    t.test_buffer_batching()
    t.test_backpressure()
