import os
import signal
import unittest
import time
import random
from typing import Optional, Tuple, Any

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.master.worker_lifecycle_controller import WorkerLifecycleController
from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
from volga.streaming.runtime.network.network_config import DEFAULT_DATA_WRITER_CONFIG, DataWriterConfig, \
    DEFAULT_DATA_READER_CONFIG
from volga.streaming.runtime.network.remote.transfer_actor import TransferActor
from volga.streaming.runtime.network.remote.transfer_io_handlers import TransferSender, TransferReceiver
from volga.streaming.runtime.network.testing_utils import TestWriter, TestReader, start_ray_io_handler_actors


class TestRemoteTransfer(unittest.TestCase):

    def _init_ray_actors(
        self,
        num_writers: int,
        num_msgs_per_writer: int,
        writer_delay_s: float = 0,
        multinode: bool = False,
        writer_config: DataWriterConfig = DEFAULT_DATA_WRITER_CONFIG,
        job_name: Optional[str] = None,
    ) -> Tuple:
        if job_name is None:
            job_name = f'job-{int(time.time())}'
        channels = []
        readers = []
        writers = []
        if multinode:
            all_nodes = ray.nodes()
            no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))
            if len(no_head) < 2:
                raise RuntimeError(f'Not enough non-head nodes in the cluster: {len(no_head)}')
            two_nodes = random.sample(no_head, 2)
            source_node, target_node = two_nodes[0], two_nodes[1]
            source_node_id = source_node['NodeID']
            target_node_id = target_node['NodeID']
            source_node_ip = source_node['NodeManagerAddress']
            target_node_ip = target_node['NodeManagerAddress']
        else:
            source_node_id = 'node_1'
            target_node_id = 'node_2'
            source_node_ip = '127.0.0.1'
            target_node_ip = '127.0.0.1'
        port = 1234

        for _id in range(num_writers):
            channel = RemoteChannel(
                channel_id=f'ch_{_id}',
                source_local_ipc_addr=f'ipc:///tmp/source_local_{_id}',
                source_node_ip=source_node_ip,
                source_node_id=source_node_id,
                target_local_ipc_addr=f'ipc:///tmp/target_local_{_id}',
                target_node_ip=target_node_ip,
                target_node_id=target_node_id,
                port=port
            )
            channels.append(channel)
            if multinode:
                # schedule on source node
                writer = TestWriter.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=source_node_id,
                        soft=False
                    )
                ).remote(_id, job_name, [channel], writer_config, writer_delay_s)

                # schedule on target node
                reader = TestReader.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(_id, job_name, [channel], num_msgs_per_writer)
            else:
                reader = TestReader.remote(_id, job_name, [channel], num_msgs_per_writer)
                writer = TestWriter.remote(_id, job_name, [channel], writer_config, writer_delay_s)
            readers.append(reader)
            writers.append(writer)
        if multinode:
            source_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
            ).remote(job_name, 'source_transfer_actor', None, channels)
            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            ).remote(job_name, 'target_transfer_actor', channels, None)
        else:
            source_transfer_actor = TransferActor.remote(job_name, 'source_transfer_actor', None, channels)
            target_transfer_actor = TransferActor.remote(job_name, 'target_transfer_actor', channels, None)

        return readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id

    def test_n_to_n_parallel_on_ray(self, n: int = 3, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_msgs_per_writer = 1000000
        msg_size = 1024
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs_per_writer)]

        ray.init(address=ray_addr, runtime_env=runtime_env)

        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=n,
            num_msgs_per_writer=num_msgs_per_writer,
            multinode=multinode,
            writer_config=writer_config
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        time.sleep(1)
        futs = []
        start_ts = time.time()
        for _id in range(n):
            writers[_id].send_items.remote({channels[_id].channel_id: to_send})
            futs.append(readers[_id].receive_items.remote())

        # wait for finish
        for _id in range(n):
            rcvd = ray.get(futs[_id])
            assert to_send == rcvd
            print(f'assert {_id} ok')
        t = time.time() - start_ts
        throughput = (n*num_msgs_per_writer)/t
        print(f'Finised in {t}s, throughput: {throughput} msg/s')
        time.sleep(1)

        for r in readers:
            ray.get(r.close.remote())

        for w in writers:
            ray.get(w.close.remote())

        ray.get(source_transfer_actor.close.remote())
        ray.get(target_transfer_actor.close.remote())

        ray.shutdown()

    def test_n_all_to_all_on_local_ray(self, n: int, num_transfer_actors: int):
        if num_transfer_actors > n or n%num_transfer_actors != 0:
            raise RuntimeError('n%num_transfer_actors should be 0')
        num_msgs = 100000
        msg_size = 32
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]
        batch_size = 100
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size
        writer_delay_s = 0

        job_name = f'job-{int(time.time())}'

        ray.init(address='auto')
        reader_channels = {}
        writer_channels = {}
        readers = {}
        writers = {}

        source_transfer_actors = {}
        source_transfer_actor_channels = {}

        target_transfer_actors = {}
        target_transfer_actor_channels = {}

        node_ip = '127.0.0.1'
        reserved_ports = {}

        for reader_id in range(n):
            for writer_id in range(n):
                channel_id = f'ch-{reader_id}-{writer_id}'
                source_node_id = f'source-{reader_id%num_transfer_actors}'
                target_node_id = f'target-{writer_id%num_transfer_actors}'
                channel = RemoteChannel(
                    channel_id=channel_id,
                    source_local_ipc_addr=f'ipc:///tmp/source_local_{channel_id}',
                    source_node_ip=node_ip,
                    source_node_id=source_node_id,
                    target_local_ipc_addr=f'ipc:///tmp/target_local_{channel_id}',
                    target_node_ip=node_ip,
                    target_node_id=target_node_id,
                    port=WorkerLifecycleController.gen_port(f'{source_node_id}-{target_node_id}', reserved_ports)
                )
                if reader_id not in reader_channels:
                    reader_channels[reader_id] = [channel]
                else:
                    reader_channels[reader_id].append(channel)

                if writer_id not in writer_channels:
                    writer_channels[writer_id] = [channel]
                else:
                    writer_channels[writer_id].append(channel)

                if source_node_id not in source_transfer_actor_channels:
                    source_transfer_actor_channels[source_node_id] = [channel]
                else:
                    source_transfer_actor_channels[source_node_id].append(channel)

                if target_node_id not in target_transfer_actor_channels:
                    target_transfer_actor_channels[target_node_id] = [channel]
                else:
                    target_transfer_actor_channels[target_node_id].append(channel)

        for reader_id in reader_channels:
            reader = TestReader.options(num_cpus=0).remote(reader_id, job_name, reader_channels[reader_id], n*num_msgs)
            readers[reader_id] = reader

        for writer_id in writer_channels:
            writer = TestWriter.options(num_cpus=0).remote(writer_id, job_name, writer_channels[writer_id], writer_config, writer_delay_s)
            writers[writer_id] = writer

        for source_node_id in source_transfer_actor_channels:
            out_channels = source_transfer_actor_channels[source_node_id]
            source_transfer_actor = TransferActor.options(num_cpus=0).remote(job_name, f'source_transfer_actor_{source_node_id}', None, out_channels)
            source_transfer_actors[source_node_id] = source_transfer_actor

        for target_node_id in target_transfer_actor_channels:
            in_channels = target_transfer_actor_channels[target_node_id]
            target_transfer_actor = TransferActor.options(num_cpus=0).remote(job_name, f'target_transfer_actor_{target_node_id}', in_channels, None)
            target_transfer_actors[target_node_id] = target_transfer_actor

        actors = list(readers.values()) + list(writers.values()) + list(source_transfer_actors.values()) + list(target_transfer_actors.values())
        start_ray_io_handler_actors(actors)
        # start_ts = time.time()
        read_futs = {}
        for writer_id in writers:
            writers[writer_id].send_items.remote({channel.channel_id: to_send for channel in writer_channels[writer_id]})

        for reader_id in readers:
            read_futs[reader_id] = readers[reader_id].receive_items.remote()

        # wait for finish
        for reader_id in read_futs:
            rcvd = ray.get(read_futs[reader_id])
            assert n * len(to_send) == len(rcvd)
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

    def test_transfer_actor_interruption(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        job_name = f'job-{int(time.time())}'
        num_msgs = 1000000
        msg_size = 1024
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size
        writer_delay_s = 0
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]

        ray.init(address=ray_addr, runtime_env=runtime_env)
        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=1,
            num_msgs_per_writer=num_msgs,
            writer_delay_s=writer_delay_s,
            multinode=multinode,
            writer_config=writer_config,
            job_name=job_name
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        writers[0].send_items.remote({channels[0].channel_id: to_send})
        fut = readers[0].receive_items.remote()

        time.sleep(1)
        i = 0
        timeout = 120
        t = time.time()
        while len(ray.get(readers[0].get_items.remote())) != len(to_send):
            if t - time.time() > timeout:
                raise RuntimeError('Timeout waiting for finish')
            if i%2 == 0:
                ray.kill(source_transfer_actor)
                time.sleep(0.2)
                ray.kill(target_transfer_actor)
                print('graceful kill')
            else:
                source_transfer_actor_pid = ray.get(source_transfer_actor.get_pid.remote())
                time.sleep(0.2)
                target_transfer_actor_pid = ray.get(target_transfer_actor.get_pid.remote())
                os.kill(source_transfer_actor_pid, signal.SIGKILL)
                os.kill(target_transfer_actor_pid, signal.SIGKILL)
                print('SIGKILL kill')

            time.sleep(1)

            if multinode:
                source_transfer_actor = TransferActor.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=source_node_id,
                        soft=False
                    )
                ).remote(job_name, 'source_transfer_actor', None, channels)

                target_transfer_actor = TransferActor.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(job_name, 'target_transfer_actor', channels, None)
            else:
                source_transfer_actor = TransferActor.remote(job_name, 'source_transfer_actor', None, channels)
                target_transfer_actor = TransferActor.remote(job_name, 'target_transfer_actor', channels, None)

            start_ray_io_handler_actors([source_transfer_actor, target_transfer_actor])
            print('re-started')
            i += 1
            time.sleep(5)

        rcvd = ray.get(fut)
        assert to_send == rcvd
        print('assert ok')

    def test_backpressure(self):
        channel = RemoteChannel(
            channel_id='1',
            source_local_ipc_addr=f'ipc:///tmp/source_local',
            source_node_ip='127.0.0.1',
            source_node_id='1',
            target_local_ipc_addr=f'ipc:///tmp/target_local',
            target_node_ip='127.0.0.1',
            target_node_id='2',
            port=1234
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
        transfer_sender = TransferSender(job_name=job_name, name='test-sender', channels=[channel])
        transfer_receiver = TransferReceiver(job_name=job_name, name='test-sender', channels=[channel])

        io_loop.register_io_handler(data_writer)
        io_loop.register_io_handler(data_reader)
        io_loop.register_io_handler(transfer_sender)
        io_loop.register_io_handler(transfer_receiver)
        io_loop.start()
        try:
            for i in range(max_buffers_per_channel + output_queue_size):
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
    t = TestRemoteTransfer()
    # t.test_n_to_n_parallel_on_ray(n=5)
    # t.test_transfer_actor_interruption()
    # t.test_n_all_to_all_on_local_ray(n=4, num_transfer_actors=2)
    t.test_backpressure()

