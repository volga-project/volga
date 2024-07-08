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

from volga.streaming.runtime.network.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.testing_utils import write, read, TestReader, TestWriter, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, start_ray_io_handler_actors
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
from volga.streaming.runtime.network.transfer.remote.remote_transfer_handler import TransferSender, TransferReceiver
from volga.streaming.runtime.network.transfer.remote.transfer_actor import TransferActor


class TestRemoteTransfer(unittest.TestCase):

    def _init_ray_actors(
        self,
        num_items: int,
        writer_delay_s: float = 0,
        multinode: bool = False,
        job_name: Optional[str] = None,
        buffering_policy: BufferingPolicy = BufferPerMessagePolicy(),
        buffering_config: BufferingConfig = BufferingConfig()
    ) -> Tuple:
        if job_name is None:
            job_name = f'job-{int(time.time())}'
        channel_id = 'ch_0'
        if multinode:
            all_nodes = ray.nodes()
            no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))
            if len(no_head) < 2:
                raise RuntimeError(f'Not enough non-head nodes in the cluster: {len(no_head)}')
            two_nodes = random.sample(no_head, 2)
            source_node, target_node = two_nodes[0], two_nodes[1]
            source_node_id = source_node['NodeID']
            target_node_id = target_node['NodeID']
            channel = RemoteChannel(
                channel_id=channel_id,
                source_local_ipc_addr='ipc:///tmp/source_local',
                source_node_ip=source_node['NodeManagerAddress'],
                source_node_id=source_node_id,
                target_local_ipc_addr='ipc:///tmp/target_local',
                target_node_ip=target_node['NodeManagerAddress'],
                target_node_id=target_node_id,
                port=1234
            )
            # schedule on source node
            writer = TestWriter.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
            ).remote(job_name, channel, writer_delay_s, buffering_policy, buffering_config)
            source_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
            ).remote(None, [channel])

            # schedule on target node
            reader = TestReader.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            ).remote(job_name, channel, num_items)
            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            ).remote([channel], None)
        else:
            source_node_id = 'node_1'
            target_node_id = 'node_2'
            channel = RemoteChannel(
                channel_id=channel_id,
                source_local_ipc_addr='ipc:///tmp/source_local',
                source_node_ip='127.0.0.1',
                source_node_id=source_node_id,
                target_local_ipc_addr='ipc:///tmp/target_local',
                target_node_ip='127.0.0.1',
                target_node_id=target_node_id,
                port=1234
            )
            reader = TestReader.remote(job_name, channel, num_items)
            writer = TestWriter.remote(job_name, channel, writer_delay_s, buffering_policy, buffering_config)
            source_transfer_actor = TransferActor.remote(job_name, 'source_transfer_actor', None, [channel])
            target_transfer_actor = TransferActor.remote(job_name, 'target_transfer_actor', [channel], None)

        return reader, writer, source_transfer_actor, target_transfer_actor, channel, source_node_id, target_node_id

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_items = 100000000
        to_send = [{'i': i} for i in range(num_items)]

        buffering_config = BufferingConfig(buffer_size=32 * 1024, capacity_per_in_channel=10, capacity_per_out=10)
        buffering_policy = PeriodicPartialFlushPolicy(0.1)

        ray.init(address=ray_addr, runtime_env=runtime_env)

        reader, writer, source_transfer_actor, target_transfer_actor, channel, source_node_id, target_node_id = self._init_ray_actors(
            num_items=num_items,
            multinode=multinode,
            buffering_policy=buffering_policy,
            buffering_config=buffering_config
        )
        start_ray_io_handler_actors([reader, writer, source_transfer_actor, target_transfer_actor])

        time.sleep(1)
        writer.send_items.remote(to_send)
        rcvd = ray.get(reader.receive_items.remote())
        time.sleep(1)

        transfer_sender_stats, _ = ray.get(source_transfer_actor.get_stats.remote())
        _, transfer_receiver_stats = ray.get(target_transfer_actor.get_stats.remote())

        print(f'TransferSender stats: {transfer_sender_stats}')
        print(f'TransferReceiver stats: {transfer_receiver_stats}')

        assert to_send == rcvd

        print('assert ok')

        time.sleep(15)

        ray.shutdown()

    def test_one_to_one_locally(self):
        num_items = 1000
        source_node_id = 'node_1'
        target_node_id = 'node_2'
        channel = RemoteChannel(
            channel_id='ch_0',
            source_local_ipc_addr='ipc:///tmp/source_local',
            source_node_ip='127.0.0.1',
            source_node_id=source_node_id,
            target_local_ipc_addr='ipc:///tmp/target_local',
            target_node_ip='127.0.0.1',
            target_node_id=target_node_id,
            port=1234
        )
        job_name = f'job-{int(time.time())}'
        io_loop = IOLoop()
        zmq_ctx = zmq.Context.instance(io_threads=10)
        data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            job_name=job_name,
            channels=[channel],
            node_id=source_node_id,
            zmq_ctx=zmq_ctx
        )
        data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            job_name=job_name,
            node_id=target_node_id,
            zmq_ctx=zmq_ctx
        )
        transfer_sender = TransferSender(
            job_name=job_name,
            name='test_transfer_sender',
            channels=[channel],
            zmq_ctx=zmq_ctx
        )
        transfer_receiver = TransferReceiver(
            job_name=job_name,
            name='test_transfer_receiver',
            channels=[channel],
            zmq_ctx=zmq_ctx
        )
        io_loop.register(data_writer)
        io_loop.register(data_reader)
        io_loop.register(transfer_sender)
        io_loop.register(transfer_receiver)
        io_loop.start()

        to_send = [{'i': i} for i in range(num_items)]
        rcvd = []

        wt = Thread(target=functools.partial(write, to_send, data_writer, channel))
        wt.start()
        read(rcvd, data_reader, num_items)
        time.sleep(1)

        transfer_sender_stats = transfer_sender.stats
        transfer_receiver_stats = transfer_receiver.stats

        print(f'TransferSender stats: {transfer_sender_stats}')
        print(f'TransferSender stats: {transfer_receiver_stats}')

        assert to_send == rcvd

        print('assert ok')
        io_loop.close()
        wt.join(5)

    def test_transfer_actor_interruption(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        job_name = f'job-{int(time.time())}'
        num_items = 2000
        writer_delay_s = 0
        to_send = [{'i': i} for i in range(num_items)]

        ray.init(address=ray_addr, runtime_env=runtime_env)
        reader, writer, source_transfer_actor, target_transfer_actor, channel, source_node_id, target_node_id = self._init_ray_actors(
            num_items=num_items,
            writer_delay_s=writer_delay_s,
            multinode=multinode,
            job_name=job_name
        )
        start_ray_io_handler_actors([reader, writer, source_transfer_actor, target_transfer_actor])

        writer.send_items.remote(to_send)
        fut = reader.receive_items.remote()

        time.sleep(1)
        i = 0
        timeout = 120
        t = time.time()
        while len(ray.get(reader.get_items.remote())) != len(to_send):
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
                ).remote(job_name, 'source_transfer_actor', None, [channel])

                target_transfer_actor = TransferActor.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(job_name, 'target_transfer_actor', [channel], None)
            else:
                source_transfer_actor = TransferActor.remote(None, [channel])
                target_transfer_actor = TransferActor.remote([channel], None)

            start_ray_io_handler_actors([source_transfer_actor, target_transfer_actor])
            print('re-started')
            i += 1
            time.sleep(1)

        rcvd = ray.get(fut)
        assert to_send == rcvd
        print('assert ok')


if __name__ == '__main__':
    t = TestRemoteTransfer()
    # t.test_one_to_one_locally()
    t.test_one_to_one_on_ray()
    # t.test_one_to_one_on_ray(ray_addr='ray://127.0.0.1:12345', runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=False)
    # t.test_transfer_actor_interruption()
