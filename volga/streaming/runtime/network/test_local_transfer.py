import random
import unittest
from typing import Optional, Any

import ray
import time

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.network.channel import LocalChannel
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
from volga.streaming.runtime.network.network_config import DEFAULT_DATA_WRITER_CONFIG, DEFAULT_DATA_READER_CONFIG
from volga.streaming.runtime.network.testing_utils import TestReader, TestWriter, start_ray_io_handler_actors


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init(address=ray_addr, runtime_env=runtime_env)
        num_msgs = 1000000
        msg_size = 128
        # to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]
        batch_size = 100
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size
        writer_delay_s = 0

        job_name = f'job-{int(time.time())}'
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
        ).remote(0, job_name, [channel])
        writer = TestWriter.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(0, job_name, [channel], writer_config, writer_delay_s)

        start_ray_io_handler_actors([reader, writer])

        # make sure Ray has enough time to start actors
        time.sleep(1)
        t = time.perf_counter()
        writer.send_items.remote({channel.channel_id: num_msgs}, msg_size)
        rcvd = ray.get(reader.receive_items.remote(num_msgs))
        t = time.perf_counter() - t
        throughput = num_msgs/t
        print(f'Finished in {t} s, throughput {throughput} msg/s')
        time.sleep(1)

        assert rcvd is True

        print('assert ok')

        ray.get(reader.close.remote())
        ray.get(writer.close.remote())

        time.sleep(1)

        ray.shutdown()

    def test_n_all_to_all_on_local_ray(self, n: int):
        num_msgs = 1000000
        msg_size = 32
        # to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size
        writer_delay_s = 0

        job_name = f'job-{int(time.time())}'

        ray.init(address='auto')
        reader_channels = {}
        writer_channels = {}
        readers = {}
        writers = {}

        for reader_id in range(n):
            for writer_id in range(n):
                channel_id = f'ch-{reader_id}-{writer_id}'
                channel = LocalChannel(
                    channel_id=channel_id,
                    ipc_addr=f'ipc:///tmp/zmqtest-{channel_id}',
                )
                if reader_id not in reader_channels:
                    reader_channels[reader_id] = [channel]
                else:
                    reader_channels[reader_id].append(channel)

                if writer_id not in writer_channels:
                    writer_channels[writer_id] = [channel]
                else:
                    writer_channels[writer_id].append(channel)

        for reader_id in reader_channels:
            reader = TestReader.options(num_cpus=0).remote(reader_id, job_name, reader_channels[reader_id])
            readers[reader_id] = reader

        for writer_id in writer_channels:
            writer = TestWriter.options(num_cpus=0).remote(writer_id, job_name, writer_channels[writer_id], writer_config, writer_delay_s)
            writers[writer_id] = writer

        actors = list(readers.values()) + list(writers.values())
        start_ray_io_handler_actors(actors)
        # start_ts = time.time()
        read_futs = {}
        for writer_id in writers:
            writers[writer_id].send_items.remote({channel.channel_id: num_msgs for channel in writer_channels[writer_id]}, msg_size)

        for reader_id in readers:
            read_futs[reader_id] = readers[reader_id].receive_items.remote(n*num_msgs)

        # wait for finish
        for reader_id in read_futs:
            rcvd = ray.get(read_futs[reader_id])
            assert rcvd is True
            print(f'assert {reader_id} ok')
        # t = time.time() - start_ts
        # throughput = (n * num_msgs_per_writer) / t
        # print(f'Finised in {t}s, throughput: {throughput} msg/s')
        time.sleep(1)

        for reader_id in readers:
            ray.get(readers[reader_id].close.remote())

        for writer_id in writers:
            ray.get(writers[writer_id].close.remote())

        ray.shutdown()

    def test_backpressure(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )

        io_loop = IOLoop(name='test_ioloop')

        job_name = f'job-{int(time.time())}'
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        max_buffers_per_channel = 5
        batch_size = 1
        writer_config.max_buffers_per_channel = max_buffers_per_channel
        writer_config.batch_size = batch_size

        reader_config = DEFAULT_DATA_READER_CONFIG
        output_queue_size = 8
        reader_config.output_queue_size = output_queue_size


        data_writer = DataWriter(name='test_writer', source_stream_name='0', job_name=job_name, channels=[channel], config=writer_config)
        data_reader = DataReader(name='test_reader', job_name=job_name, channels=[channel], config=reader_config)
        io_loop.register_io_handler(data_writer)
        io_loop.register_io_handler(data_reader)
        err = io_loop.connect_and_start()
        if err is not None:
            raise RuntimeError(f"Unable to start io_loop {io_loop.name}: {err}")
        try:
            for _ in range(max_buffers_per_channel + output_queue_size):
                time.sleep(0.1)
                s = data_writer.try_write_message(channel_id=channel.channel_id, message={'k': 'v'})
                assert s is True

            time.sleep(0.1)
            s = data_writer.try_write_message(channel_id=channel.channel_id, message={'k': 'v'})
            # should backpressure
            assert s is False

            # read one
            time.sleep(0.1)
            data_reader.read_message()
            time.sleep(0.1)
            s = data_writer.try_write_message(channel_id=channel.channel_id, message={'k': 'v'})
            # should not backpressure
            assert s is True

            time.sleep(0.1)
            s = data_writer.try_write_message(channel_id=channel.channel_id, message={'k': 'v'})
            # should backpressure
            assert s is False

            # TODO test queue lengths
            print('assert ok')
        finally:
            io_loop.close()



if __name__ == '__main__':
    t = TestLocalTransfer()
    t.test_one_to_one_on_ray()
    t.test_n_all_to_all_on_local_ray(n=4)
    t.test_backpressure()
