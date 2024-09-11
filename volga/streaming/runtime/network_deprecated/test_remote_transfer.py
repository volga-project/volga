import functools
import os
import random
import signal
import time
import unittest
from threading import Thread
from typing import Optional, Any, Tuple

import ray
import zmq
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from volga.streaming.runtime.network.testing_utils import RAY_ADDR

from volga.streaming.runtime.network_deprecated.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network_deprecated.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network_deprecated.channel import RemoteChannel
from volga.streaming.runtime.network_deprecated.testing_utils import write, read, TestReader, TestWriter, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, start_ray_io_handler_actors
from volga.streaming.runtime.network_deprecated.transfer.io_loop import IOLoop
from volga.streaming.runtime.network_deprecated.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network_deprecated.transfer.local.data_writer import DataWriter
from volga.streaming.runtime.network_deprecated.transfer.remote.remote_transfer_handler import TransferSender, TransferReceiver
from volga.streaming.runtime.network_deprecated.transfer.remote.transfer_actor import TransferActor


class TestRemoteTransfer(unittest.TestCase):

    def _init_ray_actors(
        self,
        num_actors: int,
        num_items_per_channel: int,
        writer_delay_s: float = 0,
        multinode: bool = False,
        job_name: Optional[str] = None,
        buffering_policy: BufferingPolicy = BufferPerMessagePolicy(),
        buffering_config: BufferingConfig = BufferingConfig()
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

        for _id in range(num_actors):
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
                ).remote(job_name, channel, writer_delay_s, buffering_policy, buffering_config)

                # schedule on target node
                reader = TestReader.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(job_name, channel, num_items_per_channel)
            else:
                reader = TestReader.remote(job_name, channel, num_items_per_channel)
                writer = TestWriter.remote(job_name, channel, writer_delay_s, buffering_policy, buffering_config)
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

    def test_n_to_n_on_ray(self, n: int = 3, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_items_per_channel = 100000000
        to_send = [{'i': i} for i in range(num_items_per_channel)]

        buffering_config = BufferingConfig(buffer_size=32 * 1024, capacity_per_in_channel=100, capacity_per_out=100)
        buffering_policy = PeriodicPartialFlushPolicy(0.1)

        ray.init(address=ray_addr, runtime_env=runtime_env)

        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_actors=n,
            num_items_per_channel=num_items_per_channel,
            multinode=multinode,
            buffering_policy=buffering_policy,
            buffering_config=buffering_config
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        time.sleep(1)
        futs = []
        for _id in range(n):
            writers[_id].send_items.remote(to_send)
            futs.append(readers[_id].receive_items.remote())

        # wait for finish
        for _id in range(n):
            rcvd = ray.get(futs[_id])
            assert to_send == rcvd
            print(f'assert {_id} ok')
        time.sleep(1)

        transfer_sender_stats, _ = ray.get(source_transfer_actor.get_stats.remote())
        _, transfer_receiver_stats = ray.get(target_transfer_actor.get_stats.remote())

        print(f'TransferSender stats: {transfer_sender_stats}')
        print(f'TransferReceiver stats: {transfer_receiver_stats}')
        print('assert all ok')

        ray.shutdown()

    def test_n_to_n_locally(self, n: int = 3):
        # simulates n 1:1 connections multiplexed via single transfer connection
        num_items_per_channel = 1000
        source_node_id = 'node_1'
        target_node_id = 'node_2'
        channels = []
        for _id in range(n):
            channels.append(RemoteChannel(
                channel_id=f'ch_{_id}',
                source_local_ipc_addr=f'ipc:///tmp/source_local_{_id}',
                source_node_ip='127.0.0.1',
                source_node_id=source_node_id,
                target_local_ipc_addr=f'ipc:///tmp/target_local_{_id}',
                target_node_ip='127.0.0.1',
                target_node_id=target_node_id,
                port=1234
            ))
        job_name = f'job-{int(time.time())}'
        io_loop = IOLoop()
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writers = []
        data_readers = []
        for _id in range(n):
            data_writers.append(DataWriter(
                name='test_writer',
                source_stream_name='0',
                job_name=job_name,
                channels=[channels[_id]],
                node_id=source_node_id,
                zmq_ctx=zmq_ctx
            ))
            data_readers.append(DataReader(
                name='test_reader',
                channels=[channels[_id]],
                job_name=job_name,
                node_id=target_node_id,
                zmq_ctx=zmq_ctx
            ))
        transfer_sender = TransferSender(
            job_name=job_name,
            name='test_transfer_sender',
            channels=channels,
            zmq_ctx=zmq_ctx
        )
        transfer_receiver = TransferReceiver(
            job_name=job_name,
            name='test_transfer_receiver',
            channels=channels,
            zmq_ctx=zmq_ctx
        )
        for _id in range(n):
            io_loop.register(data_writers[_id])
            io_loop.register(data_readers[_id])
        io_loop.register(transfer_sender)
        io_loop.register(transfer_receiver)
        io_loop.start()

        to_send = {_id: [{'i': i} for i in range(num_items_per_channel)] for _id in range(n)}
        rcvd = {_id: [] for _id in range(n)}
        read_threads = []
        for _id in range(n):
            wt = Thread(target=functools.partial(write, to_send[_id], data_writers[_id], channels[_id]))
            rt = Thread(target=functools.partial(read, rcvd[_id], data_readers[_id], num_items_per_channel))
            read_threads.append(rt)
            wt.start()
            rt.start()
        time.sleep(1)

        # wait for all reads to finnish
        for _id in range(n):
            rt = read_threads[_id]
            rt.join()
            assert to_send[_id] == rcvd[_id]
            print(f'assert {_id} ok')

        transfer_sender_stats = transfer_sender.stats
        transfer_receiver_stats = transfer_receiver.stats

        print(f'TransferSender stats: {transfer_sender_stats}')
        print(f'TransferSender stats: {transfer_receiver_stats}')

        print('assert all ok')
        io_loop.close()

    def test_transfer_actor_interruption(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        job_name = f'job-{int(time.time())}'
        num_items = 2000
        writer_delay_s = 0
        to_send = [{'i': i} for i in range(num_items)]

        ray.init(address=ray_addr, runtime_env=runtime_env)
        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_actors=1,
            num_items_per_channel=num_items,
            writer_delay_s=writer_delay_s,
            multinode=multinode,
            job_name=job_name
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        writers[0].send_items.remote(to_send)
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
            time.sleep(1)

        rcvd = ray.get(fut)
        assert to_send == rcvd
        print('assert ok')


if __name__ == '__main__':
    t = TestRemoteTransfer()
    # t.test_n_to_n_locally(n=3)
    # t.test_n_to_n_on_ray(n=1)
    t.test_n_to_n_on_ray(n=1, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
    # t.test_transfer_actor_interruption()
