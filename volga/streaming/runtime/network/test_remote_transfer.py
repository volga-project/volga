import functools
import random
import time
import unittest
from threading import Thread
from typing import Optional, Any

import ray
import zmq
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.stats import StatsEvent
from volga.streaming.runtime.network.testing_utils import write, read, TestReader, TestWriter, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.streaming.runtime.network.transfer.io_loop import IOLoop, Direction
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
from volga.streaming.runtime.network.transfer.remote.remote_transfer_handler import RemoteTransferHandler
from volga.streaming.runtime.network.transfer.remote.transfer_actor import TransferActor

import ray.util.state as ray_state


class TestRemoteTransfer(unittest.TestCase):

    def test_one_to_one_on_ray(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_items = 1000
        to_send = [{'i': i} for i in range(num_items)]

        ray.init(address=ray_addr, runtime_env=runtime_env)

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
            ).remote(channel)
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
            ).remote(job_name, [channel], None)
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
            writer = TestWriter.remote(job_name, channel)
            source_transfer_actor = TransferActor.remote(None, [channel])
            target_transfer_actor = TransferActor.remote([channel], None)

        source_transfer_actor.start.remote()
        target_transfer_actor.start.remote()

        time.sleep(1)
        writer.send_items.remote(to_send)
        rcvd = ray.get(reader.receive_items.remote())
        time.sleep(1)

        transfer_sender_stats, _ = ray.get(source_transfer_actor.get_stats.remote())
        _, transfer_receiver_stats = ray.get(target_transfer_actor.get_stats.remote())

        assert to_send == sorted(rcvd, key=lambda e: e['i'])

        assert transfer_sender_stats.get_counter_for_event(StatsEvent.MSG_SENT)[target_node_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[target_node_id] == num_items

        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[source_node_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.ACK_SENT)[source_node_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items

        print('assert ok')

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
        transfer_sender = RemoteTransferHandler(
            channels=[channel],
            zmq_ctx=zmq_ctx,
            direction=Direction.SENDER
        )
        transfer_receiver = RemoteTransferHandler(
            channels=[channel],
            zmq_ctx=zmq_ctx,
            direction=Direction.RECEIVER
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

        assert transfer_sender_stats.get_counter_for_event(StatsEvent.MSG_SENT)[target_node_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[channel.channel_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.ACK_SENT)[channel.channel_id] == num_items
        assert transfer_sender_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[target_node_id] == num_items

        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.MSG_SENT)[channel.channel_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.MSG_RCVD)[source_node_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.ACK_SENT)[source_node_id] == num_items
        assert transfer_receiver_stats.get_counter_for_event(StatsEvent.ACK_RCVD)[channel.channel_id] == num_items

        print('assert ok')
        io_loop.close()
        wt.join(5)


if __name__ == '__main__':
    t = TestRemoteTransfer()
    t.test_one_to_one_locally()
    t.test_one_to_one_on_ray()
    # t.test_one_to_one_on_ray(ray_addr='ray://127.0.0.1:12345', runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=False)
