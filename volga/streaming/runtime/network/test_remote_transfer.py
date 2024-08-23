import os
import signal
import unittest
import time
import random
from typing import Optional, Tuple, Any

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.remote.transfer_actor import TransferActor
from volga.streaming.runtime.network.testing_utils import TestWriter, TestReader, start_ray_io_handler_actors


class TestRemoteTransfer(unittest.TestCase):

    def _init_ray_actors(
        self,
        num_writers: int,
        num_msgs_per_writer: int,
        writer_delay_s: float = 0,
        batch_size: int = 1000,
        multinode: bool = False,
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
                ).remote(_id, job_name, [channel], batch_size, writer_delay_s)

                # schedule on target node
                reader = TestReader.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(_id, job_name, [channel], num_msgs_per_writer)
            else:
                reader = TestReader.remote(_id, job_name, [channel], num_msgs_per_writer)
                writer = TestWriter.remote(_id, job_name, [channel], batch_size, writer_delay_s)
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
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs_per_writer)]

        ray.init(address=ray_addr, runtime_env=runtime_env)

        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=n,
            num_msgs_per_writer=num_msgs_per_writer,
            multinode=multinode,
            batch_size=batch_size
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

    def test_transfer_actor_interruption(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        job_name = f'job-{int(time.time())}'
        num_msgs = 1000000
        msg_size = 1024
        batch_size = 1000
        writer_delay_s = 0
        to_send = [{'i': str(random.randint(0, 9)) * msg_size} for _ in range(num_msgs)]

        ray.init(address=ray_addr, runtime_env=runtime_env)
        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=1,
            num_msgs_per_writer=num_msgs,
            batch_size=batch_size,
            writer_delay_s=writer_delay_s,
            multinode=multinode,
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


if __name__ == '__main__':
    t = TestRemoteTransfer()
    t.test_n_to_n_parallel_on_ray(n=5)
    # t.test_transfer_actor_interruption()
