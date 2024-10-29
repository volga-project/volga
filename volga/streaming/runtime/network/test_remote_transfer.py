import os
import signal
import socket
import unittest
import time
import random
import volga
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
from volga.streaming.runtime.network.testing_utils import TestWriter, TestReader, start_ray_io_handler_actors, RAY_ADDR, \
    REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV


class TestRemoteTransfer(unittest.TestCase):

    def _init_ray_actors(
        self,
        num_writers: int,
        multinode: bool = False,
        writer_config: DataWriterConfig = DEFAULT_DATA_WRITER_CONFIG,
        job_name: Optional[str] = None,
    ) -> Tuple:
        if job_name is None:
            job_name = f'job-{int(time.time())}'
        channels = []
        readers = []
        writers = []
        all_nodes = ray.nodes()
        no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))
        single_node_id = None
        if multinode:
            if len(no_head) < 2:
                raise RuntimeError(f'Not enough non-head nodes in the cluster: {len(no_head)}')
            two_nodes = random.sample(no_head, 2)
            source_node, target_node = two_nodes[0], two_nodes[1]
            source_node_id = source_node['NodeID']
            target_node_id = target_node['NodeID']
            source_node_ip = source_node['NodeManagerAddress']
            target_node_ip = target_node['NodeManagerAddress']
        else:
            if len(no_head) != 0:
                node = no_head[0]
            else:
                node = all_nodes[0]
            single_node_id = node['NodeID']
            source_node_id = 'node_1'
            target_node_id = 'node_2'
            source_node_ip = '127.0.0.1'
            target_node_ip = '127.0.0.1'
        port = 1234
        handler_id = 0

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
                ).remote(handler_id, job_name, [channel], writer_config)
                handler_id += 1

                # schedule on target node
                reader = TestReader.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id,
                        soft=False
                    )
                ).remote(handler_id, job_name, [channel])
                handler_id += 1
            else:
                reader = TestReader.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=single_node_id,
                        soft=False
                    )
                ).remote(handler_id, job_name, [channel])
                handler_id += 1

                writer = TestWriter.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=single_node_id,
                        soft=False
                    )
                ).remote(handler_id, job_name, [channel], writer_config)
                handler_id += 1

            readers.append(reader)
            writers.append(writer)
        if multinode:
            source_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
            ).remote(job_name, 'source_transfer_actor', None, handler_id, None, channels)
            handler_id += 1
            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            ).remote(job_name, 'target_transfer_actor', handler_id, None, channels, None)
            handler_id += 1
        else:
            source_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=single_node_id,
                    soft=False
                )
            ).remote(job_name, 'source_transfer_actor', None, handler_id, None, channels)
            handler_id += 1

            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=single_node_id,
                    soft=False
                )
            ).remote(job_name, 'target_transfer_actor', handler_id, None, channels, None)
            handler_id += 1

        return readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id

    def test_n_to_n_parallel_on_ray(self, n: int = 3, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_msgs_per_writer = 5000000
        msg_size = 32
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size

        ray.init(address=ray_addr, runtime_env=runtime_env)

        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=n,
            multinode=multinode,
            writer_config=writer_config
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        time.sleep(1)
        futs = []
        start_ts = time.time()
        for i in range(n):
            writers[i].send_items.remote({channels[i].channel_id: num_msgs_per_writer}, msg_size)
            futs.append(readers[i].receive_items.remote(num_msgs_per_writer))

        # wait for finish
        for i in range(n):
            rcvd = ray.get(futs[i])
            assert rcvd is True
            print(f'assert {i} ok')
        t = time.time() - start_ts
        throughput = (n*num_msgs_per_writer)/t
        print(f'Finised in {t}s, throughput: {throughput} msg/s')
        time.sleep(1)

        ray.get([r.stop.remote() for r in readers])
        ray.get([w.stop.remote() for w in writers])

        ray.get(source_transfer_actor.stop.remote())
        ray.get(target_transfer_actor.stop.remote())

        ray.shutdown()

    # reader/writer + transfer per node, star topology (nw*nr)
    def test_nw_to_nr_star_on_ray(self, nw: int, nr: int, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False) -> Tuple:
        num_msgs = 100000
        msg_size = 32
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size

        job_name = f'job-{int(time.time())}'

        ray.init(address=ray_addr, runtime_env=runtime_env)
        reader_channels = {}
        writer_channels = {}
        readers = {}
        writers = {}
        nodes_per_reader = {}
        nodes_per_writer = {}

        source_transfer_actors = {}
        source_transfer_actor_channels = {}

        target_transfer_actors = {}
        target_transfer_actor_channels = {}

        reserved_ports = {}

        all_nodes = ray.nodes()
        no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))

        if multinode:
            if len(no_head) < nr + nw:
                raise RuntimeError(f'Not enough non-head nodes in the cluster: {len(no_head)}, expected {nr + nw}')
            node_index = 0
            for reader_id in range(nr):
                nodes_per_reader[reader_id] = no_head[node_index]
                node_index += 1

            for writer_id in range(nw):
                nodes_per_writer[writer_id] = no_head[node_index]
                node_index += 1

        for reader_id in range(nr):
            for writer_id in range(nw):
                channel_id = f'ch-{writer_id}-{reader_id}'
                if multinode:
                    source_node = nodes_per_writer[writer_id]
                    target_node = nodes_per_reader[reader_id]
                    source_node_id = source_node['NodeID']
                    target_node_id = target_node['NodeID']
                    source_node_ip = source_node['NodeManagerAddress']
                    target_node_ip = target_node['NodeManagerAddress']
                else:
                    source_node_id = f'source-{writer_id}'
                    target_node_id = f'target-{reader_id}'
                    source_node_ip = '127.0.0.1'
                    target_node_ip = '127.0.0.1'

                # use '' as node_id because we run test on one node and can not have duplicate ports
                port = WorkerLifecycleController.gen_port(f'{source_node_id}-{target_node_id}', target_node_id, reserved_ports, {})
                channel = RemoteChannel(
                    channel_id=channel_id,
                    source_local_ipc_addr=f'ipc:///tmp/source_local_{channel_id}',
                    source_node_ip=source_node_ip,
                    source_node_id=source_node_id,
                    target_local_ipc_addr=f'ipc:///tmp/target_local_{channel_id}',
                    target_node_ip=target_node_ip,
                    target_node_id=target_node_id,
                    port=port
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
            options = {'num_cpus': 0}
            if multinode:
                node_id = nodes_per_reader[reader_id]['NodeID']
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False
                )
            reader = TestReader.options(**options).remote(reader_id, job_name, reader_channels[reader_id])
            readers[reader_id] = reader

        for writer_id in writer_channels:
            options = {'num_cpus': 0}
            if multinode:
                node_id = nodes_per_writer[writer_id]['NodeID']
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False
                )
            writer = TestWriter.options(**options).remote(writer_id, job_name, writer_channels[writer_id], writer_config)
            writers[writer_id] = writer

        for source_node_id in source_transfer_actor_channels:
            options = {'num_cpus': 0}
            if multinode:
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
            out_channels = source_transfer_actor_channels[source_node_id]
            source_transfer_actor = TransferActor.options(**options).remote(job_name, f'source_transfer_actor_{source_node_id}', None, out_channels)
            source_transfer_actors[source_node_id] = source_transfer_actor

        for target_node_id in target_transfer_actor_channels:
            options = {'num_cpus': 0}
            if multinode:
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            in_channels = target_transfer_actor_channels[target_node_id]
            target_transfer_actor = TransferActor.options(**options).remote(job_name, f'target_transfer_actor_{target_node_id}', in_channels, None)
            target_transfer_actors[target_node_id] = target_transfer_actor

        actors = list(readers.values()) + list(writers.values()) + list(source_transfer_actors.values()) + list(target_transfer_actors.values())
        start_ray_io_handler_actors(actors)
        start_ts = time.time()
        read_futs = {}
        for writer_id in writers:
            writers[writer_id].send_items.remote({channel.channel_id: num_msgs for channel in writer_channels[writer_id]}, msg_size)

        for reader_id in readers:
            read_futs[reader_id] = readers[reader_id].receive_items.remote(nw * num_msgs)

        # wait for finish
        for reader_id in read_futs:
            rcvd = ray.get(read_futs[reader_id])
            assert rcvd is True
            print(f'assert {reader_id} ok')
        t = time.time() - start_ts
        throughput = (nw * nr * num_msgs) / t
        print(f'Finished in {t}s, throughput: {throughput} msg/s')
        time.sleep(1)

        stop_futs = []
        for reader_id in readers:
            stop_futs.append(readers[reader_id].stop.remote())

        for writer_id in writers:
            stop_futs.append(writers[writer_id].stop.remote())

        ray.get(stop_futs)

        ray.shutdown()
        return throughput, t

    def test_transfer_actor_interruption(self, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        job_name = f'job-{int(time.time())}'
        num_msgs = 1000000
        msg_size = 1024
        batch_size = 1000
        writer_config = DEFAULT_DATA_WRITER_CONFIG
        writer_config.batch_size = batch_size

        ray.init(address=ray_addr, runtime_env=runtime_env)
        readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id = self._init_ray_actors(
            num_writers=1,
            multinode=multinode,
            writer_config=writer_config,
            job_name=job_name
        )
        start_ray_io_handler_actors([*readers, *writers, source_transfer_actor, target_transfer_actor])

        writers[0].send_items.remote({channels[0].channel_id: num_msgs}, msg_size)
        fut = readers[0].receive_items.remote(num_msgs)

        time.sleep(1)
        i = 0
        timeout = 120
        t = time.time()
        while ray.get(readers[0].get_num_rcvd.remote()) != num_msgs:
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
        assert rcvd is True
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
        err = io_loop.connect_and_start()
        if err is not None:
            raise RuntimeError(f"Unable to start io_loop {io_loop.name}: {err}")
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

            print('assert ok')
        finally:
            io_loop.stop()

    def throughput_benchmark(self):
        res = {}
        for i in range(25, 71, 5):
            n = i
            if i == 0:
                n = 1
            try:
                res[i] = self.test_nw_to_nr_star_on_ray(nw=n, nr=n, ray_addr=RAY_ADDR,
                                                     runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
                time.sleep(2)
            except Exception as e:
                res[i] = (-1, -1)
                print(f'Failed {i}<->{i}: {e}')

        for i in res:
            throughput, t = res[i]
            if throughput < 0:
                print(f'{i}<->{i}: Failed')
            else:
                print(f'{i}<->{i}: {throughput} msg/s, {t} s')


if __name__ == '__main__':
    t = TestRemoteTransfer()
    # t.test_n_to_n_parallel_on_ray(n=1, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
    # t.test_n_to_n_parallel_on_ray(n=1)
    # t.test_transfer_actor_interruption()
    t.throughput_benchmark()
    # t.test_backpressure()

# 1<->1: 77279.62991009754 msg/s, 1.2940020561218262 s
# 2<->2: 159084.37156745538 msg/s, 2.5143890380859375 s
# 3<->3: 198417.67251588262 msg/s, 4.535886287689209 s
# 4<->4: 288623.8268141708 msg/s, 5.543547868728638 s
# 5<->5: 353878.0211981364 msg/s, 7.0645811557769775 s
# 6<->6: 377512.22861806024 msg/s, 9.536114931106567 s
# 7<->7: 445156.1366645908 msg/s, 11.007373809814453 s
# 8<->8: 487622.0946902425 msg/s, 13.124917984008789 s
# 9<->9: 557218.074788173 msg/s, 14.5364990234375 s
# 10<->10: 571694.5873933224 msg/s, 17.491857051849365 s
# 11<->11: 688648.7879781058 msg/s, 17.570640087127686 s
# 12<->12: 724608.2452501928 msg/s, 19.872807264328003 s
# 13<->13: 809510.9968577144 msg/s, 20.876801013946533 s
# 14<->14: 874072.8980508689 msg/s, 22.42375898361206 s
# 15<->15: 918634.05002135 msg/s, 24.492887020111084 s

# 0<->0: 78195.00091631038 msg/s, 1.2788541316986084 s
# 5<->5: 270598.81952627556 msg/s, 9.238769054412842 s
# 10<->10: 505774.6804948192 msg/s, 19.771650075912476 s
# 15<->15: 752058.2860982413 msg/s, 29.917893886566162 s
# 20<->20: 926800.8961873061 msg/s, 43.15921592712402 s

