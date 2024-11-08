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
from volga.streaming.runtime.network.testing_utils import TestReader, TestWriter, start_ray_io_handler_actors, RAY_ADDR, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV


class TestLocalTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )
        ray.init(address=ray_addr, runtime_env=runtime_env)
        run_for_s = 10

        msg_size = 128
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size

        handler_id_inc = 0

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
        ).remote(handler_id_inc, job_name, [channel])
        handler_id_inc += 1
        writer = TestWriter.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node['NodeID'],
                soft=False
            )
        ).remote(handler_id_inc, job_name, [channel], writer_config)
        handler_id_inc += 1

        start_ray_io_handler_actors([reader, writer])

        # make sure Ray has enough time to start actors
        time.sleep(1)
        t = time.perf_counter()

        w = writer.round_robin_send.remote([channel.channel_id], msg_size, run_for_s)
        r = reader.receive_items.remote(run_for_s + 5)
        res = ray.get([w, r])
        num_sent = res[0][channel.channel_id]
        num_rcvd = res[1][channel.channel_id]

        t = time.perf_counter() - t
        throughput = num_rcvd/t
        print(f'Finished in {t} s, throughput {throughput} msg/s')
        time.sleep(1)

        assert num_sent == num_rcvd
        print('assert ok')

        ray.get([reader.stop.remote(), writer.stop.remote()])
        ray.shutdown()

    def test_n_all_to_all_on_local_ray(self, n: int):
        run_for_s = 10
        msg_size = 128
        batch_size = 1000

        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size

        job_name = f'job-{int(time.time())}'

        ray.init(address='auto')
        reader_channels = {}
        writer_channels = {}
        readers = {}
        writers = {}

        reader_ids = [*range(n)]
        writer_ids = [*range(n, 2*n)]

        for reader_id in reader_ids:
            for writer_id in writer_ids:
                channel_id = f'ch-r{reader_id}-w{writer_id}'
                channel = LocalChannel(
                    channel_id=channel_id,
                    ipc_addr=f'ipc:///tmp/zmqtest-{reader_id}',
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
            writer = TestWriter.options(num_cpus=0).remote(writer_id, job_name, writer_channels[writer_id], writer_config)
            writers[writer_id] = writer

        actors = list(readers.values()) + list(writers.values())
        start_ray_io_handler_actors(actors)
        write_futs = {}
        read_futs = {}

        start_ts = time.time()
        for writer_id in writers:
            write_futs[writer_id] = writers[writer_id].round_robin_send.remote([channel.channel_id for channel in writer_channels[writer_id]], msg_size, run_for_s)

        for reader_id in readers:
            read_futs[reader_id] = readers[reader_id].receive_items.remote(run_for_s + 10)

        # wait for finish
        res = ray.get(list(write_futs.values()) + list(read_futs.values()))
        num_msgs_sent_total = {}
        num_msgs_rcvd_total = {}
        i = 0
        for _ in range(len(write_futs)):
            num_msgs_sent = res[i]
            for channel_id in num_msgs_sent:
                if channel_id in num_msgs_sent_total:
                    num_msgs_sent_total[channel_id] += num_msgs_sent[channel_id]
                else:
                    num_msgs_sent_total[channel_id] = num_msgs_sent[channel_id]
            i += 1

        for _ in range(len(read_futs)):
            num_msgs_rcvd = res[i]
            for channel_id in num_msgs_rcvd:
                if channel_id in num_msgs_rcvd_total:
                    num_msgs_rcvd_total[channel_id] += num_msgs_rcvd[channel_id]
                else:
                    num_msgs_rcvd_total[channel_id] = num_msgs_rcvd[channel_id]
            i += 1

        assert len(num_msgs_rcvd_total) == len(num_msgs_sent_total)
        for channel_id in num_msgs_rcvd_total:
            assert num_msgs_rcvd_total[channel_id] == num_msgs_sent_total[channel_id]
        print('assert ok')

        num_msgs = sum(list(num_msgs_rcvd_total.values()))
        t = time.time() - start_ts
        throughput = num_msgs / t
        print(f'Finised in {t}s, throughput: {throughput} msg/s')

        ray.get([readers[reader_id].stop.remote() for reader_id in readers] + [writers[writer_id].stop.remote() for writer_id in writers])
        ray.shutdown()

    # TODO fix this to work with memory bound queues
    def test_backpressure(self):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )

        io_loop = IOLoop(name='test_ioloop')

        job_name = f'job-{int(time.time())}'
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        max_capacity_bytes_per_channel = 50
        batch_size = 1
        writer_config.max_capacity_bytes_per_channel = max_capacity_bytes_per_channel
        writer_config.batch_size = batch_size

        reader_config = DEFAULT_DATA_READER_CONFIG
        output_queue_capacity_bytes = 8
        reader_config.output_queue_capacity_bytes = output_queue_capacity_bytes

        data_writer = DataWriter(handler_id='0', name='test_writer', source_stream_name='0', job_name=job_name, channels=[channel], config=writer_config)
        data_reader = DataReader(handler_id='1', name='test_reader', job_name=job_name, channels=[channel], config=reader_config)
        io_loop.register_io_handler(data_writer)
        io_loop.register_io_handler(data_reader)
        err = io_loop.connect_and_start()
        if err is not None:
            raise RuntimeError(f"Unable to start io_loop {io_loop.name}: {err}")
        try:
            capacity_left = max_capacity_bytes_per_channel + output_queue_capacity_bytes
            while capacity_left > 0:
                # TODO fix this
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
            io_loop.stop()



if __name__ == '__main__':
    t = TestLocalTransfer()
    # t.test_one_to_one_on_ray()
    t.test_n_all_to_all_on_local_ray(n=4)
    # t.test_backpressure() # TODO this does not work
