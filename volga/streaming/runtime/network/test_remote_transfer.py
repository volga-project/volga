import math
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

from volga.streaming.runtime.master.stats.stats_manager import StatsManager
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
                source_local_ipc_addr=f'ipc:///tmp/source_local',
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
            ).remote(job_name, 'source_transfer_actor', None, str(handler_id), None, channels)
            handler_id += 1
            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
            ).remote(job_name, 'target_transfer_actor', str(handler_id), None, channels, None)
            handler_id += 1
        else:
            source_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=single_node_id,
                    soft=False
                )
            ).remote(job_name, 'source_transfer_actor', None, str(handler_id), None, channels)
            handler_id += 1

            target_transfer_actor = TransferActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=single_node_id,
                    soft=False
                )
            ).remote(job_name, 'target_transfer_actor', str(handler_id), None, channels, None)
            handler_id += 1

        return readers, writers, source_transfer_actor, target_transfer_actor, channels, source_node_id, target_node_id

    def test_n_to_n_parallel_on_ray(self, n: int = 3, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False):
        num_msgs_per_writer = 20000000
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
        stats_manager = StatsManager()
        for reader in readers:
            stats_manager.register_worker(reader)

        time.sleep(1)
        stats_manager.start()
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

        avg_throughput, avg_latency = stats_manager.get_final_aggregated_stats()
        stats_manager.stop()

        t = time.time() - start_ts
        estimated_throughput = (n*num_msgs_per_writer)/t
        print(f'Finished in {t}s \n'
              f'Avg Throughput: {avg_throughput} msg/s \n'
              f'Estimated Throughput: {estimated_throughput} msg/s \n'
              f'Latency: {avg_latency} \n')
        time.sleep(1)

        ray.get([r.stop.remote() for r in readers])
        ray.get([w.stop.remote() for w in writers])

        ray.get(source_transfer_actor.stop.remote())
        ray.get(target_transfer_actor.stop.remote())


        ray.shutdown()

    # reader/writer + transfer per node, star topology (nw*nr)
    def test_nw_to_nr_star_on_ray(self, nw: int, nr: int, num_workers_per_node: Optional[int] = None, ray_addr: Optional[str] = None, runtime_env: Optional[Any] = None, multinode: bool = False) -> Tuple:
        num_msgs = 1000000
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

        reader_ids = [*range(nr)]
        writer_ids = [*range(nr, nr + nw)]

        nodes_per_reader = {}
        nodes_per_writer = {}

        source_transfer_actors = {}
        source_transfer_actor_channels = {}

        target_transfer_actors = {}
        target_transfer_actor_channels = {}

        all_nodes = ray.nodes()
        no_head = list(filter(lambda n: 'node:__internal_head__' not in n['Resources'], all_nodes))

        if multinode:
            expected_num_nodes = int(math.ceil(nr/num_workers_per_node)) + int(math.ceil(nw/num_workers_per_node))
            if len(no_head) < expected_num_nodes:
                raise RuntimeError(f'Not enough non-head nodes in the cluster: {len(no_head)}, expected {expected_num_nodes}')
            node_index = 0
            left_per_node = num_workers_per_node
            for reader_id in reader_ids:
                nodes_per_reader[reader_id] = no_head[node_index]
                left_per_node -= 1
                if left_per_node == 0:
                    node_index += 1
                    left_per_node = num_workers_per_node

            # move to next if current node already has workers
            if left_per_node != num_workers_per_node:
                node_index += 1

            left_per_node = num_workers_per_node
            for writer_id in writer_ids:
                nodes_per_writer[writer_id] = no_head[node_index]
                left_per_node -= 1
                if left_per_node == 0:
                    node_index += 1

        for reader_id in reader_ids:
            for writer_id in writer_ids:
                channel_id = f'ch-w{writer_id}-r{reader_id}'
                if multinode:
                    source_node = nodes_per_writer[writer_id]
                    target_node = nodes_per_reader[reader_id]
                    source_node_id = source_node['NodeID']
                    target_node_id = target_node['NodeID']
                    source_node_ip = source_node['NodeManagerAddress']
                    target_node_ip = target_node['NodeManagerAddress']
                else:
                    # source_node_id = f'source-{writer_id}'
                    # target_node_id = f'target-{reader_id}'
                    source_node_id = f'source-node'
                    target_node_id = f'target-node'
                    source_node_ip = '127.0.0.1'
                    target_node_ip = '127.0.0.1'

                channel = RemoteChannel(
                    channel_id=channel_id,
                    source_local_ipc_addr=f'ipc:///tmp/source_local_{source_node_id}',
                    source_node_ip=source_node_ip,
                    source_node_id=source_node_id,
                    target_local_ipc_addr=f'ipc:///tmp/target_local_{reader_id}',
                    target_node_ip=target_node_ip,
                    target_node_id=target_node_id,
                    port=2345
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

        handler_id = 0
        for reader_id in reader_channels:
            options = {'num_cpus': 0}
            if multinode:
                node_id = nodes_per_reader[reader_id]['NodeID']
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False
                )
            reader = TestReader.options(**options).remote(str(handler_id), job_name, reader_channels[reader_id])
            readers[reader_id] = reader
            handler_id += 1

        for writer_id in writer_channels:
            options = {'num_cpus': 0}
            if multinode:
                node_id = nodes_per_writer[writer_id]['NodeID']
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False
                )
            writer = TestWriter.options(**options).remote(str(handler_id), job_name, writer_channels[writer_id], writer_config)
            writers[writer_id] = writer
            handler_id += 1

        # configure  transfer actors:
        if multinode:
            for source_node_id in source_transfer_actor_channels:
                options = {'num_cpus': 0}
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=source_node_id,
                    soft=False
                )
                out_channels = source_transfer_actor_channels[source_node_id]
                source_transfer_actor = TransferActor.options(**options).remote(job_name, f'source_transfer_actor_{source_node_id}', None, str(handler_id), None, out_channels)
                source_transfer_actors[source_node_id] = source_transfer_actor
                handler_id += 1

            for target_node_id in target_transfer_actor_channels:
                options = {'num_cpus': 0}
                options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
                    node_id=target_node_id,
                    soft=False
                )
                in_channels = target_transfer_actor_channels[target_node_id]
                target_transfer_actor = TransferActor.options(**options).remote(job_name, f'target_transfer_actor_{target_node_id}', str(handler_id), None, in_channels, None)
                target_transfer_actors[target_node_id] = target_transfer_actor
                handler_id += 1

            actors = list(readers.values()) + list(writers.values()) + list(source_transfer_actors.values()) + list(target_transfer_actors.values())
        else:
            in_channels = list(source_transfer_actor_channels.values())[0]
            out_channels = list(target_transfer_actor_channels.values())[0]
            node_id = list(source_transfer_actor_channels.keys())[0]
            options = {'num_cpus': 0}
            transfer_actor = TransferActor.options(**options).remote(job_name, f'transfer_actor_{node_id}',
                                                                            str(handler_id), str(handler_id + 1), in_channels, out_channels)
            handler_id += 2
            actors = list(readers.values()) + list(writers.values()) + [transfer_actor]

        start_ray_io_handler_actors(actors)

        stats_manager = StatsManager()
        for reader_id in readers:
            stats_manager.register_worker(readers[reader_id])
        stats_manager.start()

        start_ts = time.time()
        read_futs = {}
        for writer_id in writers:
            writers[writer_id].send_items.remote({channel.channel_id: num_msgs for channel in writer_channels[writer_id]}, msg_size)

        for reader_id in readers:
            read_futs[reader_id] = readers[reader_id].receive_items.remote(nw * num_msgs)

        # wait for finish
        _reader_ids = list(read_futs.keys())
        _reader_futs = list(read_futs.values())
        _all_rcvd = ray.get(_reader_futs)
        for i in range(len(_reader_ids)):
            reader_id = _reader_ids[i]
            rcvd = _all_rcvd[i]
            assert rcvd is True
            print(f'assert {reader_id} ok')

        stats_manager.stop()

        avg_throughput, avg_latency = stats_manager.get_final_aggregated_stats()
        stats_manager.stop()

        t = time.time() - start_ts
        estimated_throughput = (nw * nr * num_msgs) / t

        print(f'Finished in {t}s \n'
              f'Avg Throughput: {avg_throughput} msg/s \n'
              f'Estimated Throughput: {estimated_throughput} msg/s \n'
              f'Latency: {avg_latency} \n')
        time.sleep(1)

        stop_futs = [actor.stop.remote() for actor in actors]
        ray.get(stop_futs)

        ray.shutdown()
        return avg_throughput, avg_latency, t

    # TODO fix this to work with new rust engine
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

    # TODO fix this to work with memory bound queues
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

    def throughput_benchmark(self, num_workers_per_node: int):
        res = {}
        for i in range(1, 9, 1):
            n = i
            if i == 0:
                n = 1
            try:
                res[n] = self.test_nw_to_nr_star_on_ray(
                    nw=n,
                    nr=n,
                    num_workers_per_node=num_workers_per_node,
                    ray_addr=RAY_ADDR,
                    runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV,
                    multinode=True
                )
                # TODO store results on disk
                time.sleep(2)
            except Exception as e:
                res[n] = (-1, -1, -1)
                print(f'Failed {n}<->{n}: {e}')
                ray.shutdown()

        for n in res:
            avg_throughput, latency_stats, t = res[n]
            if avg_throughput < 0:
                print(f'{n}<->{n}: Failed')
            else:
                print(f'{n}<->{n}: {avg_throughput} msg/s, {latency_stats}, {t} s')


if __name__ == '__main__':
    t = TestRemoteTransfer()
    # t.test_n_to_n_parallel_on_ray(n=2, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
    # t.test_n_to_n_parallel_on_ray(n=2)
    t.test_nw_to_nr_star_on_ray(nr=4, nw=4)
    # t.test_nw_to_nr_star_on_ray(nr=8, nw=8, num_workers_per_node=8, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
    # t.throughput_benchmark(8)

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

# -- new --
# 1<->1: 77391.47987838203 msg/s, 1.2921319007873535 s
# 5<->5: 274153.7433745098 msg/s, 9.11897087097168 s
# 10<->10: 527487.0838855837 msg/s, 18.957810163497925 s
# 15<->15: 745625.5125779167 msg/s, 30.176006078720093 s
# 20<->20: 943023.6885939682 msg/s, 42.41674995422363 s
# 25<->25: 1054430.353377645 msg/s, 59.27371096611023 s
# 30<->30: 1714816.090105296 msg/s, 52.48376226425171 s
# 35<->35: 2121138.693458819 msg/s, 57.75199913978577 s
# 40<->40: 2237966.1073087333 msg/s, 71.49348664283752 s
# 45<->45: 2472339.831106908 msg/s, 81.90621590614319 s
# 50<->50: 2785009.5203105453 msg/s, 89.76629996299744 s

