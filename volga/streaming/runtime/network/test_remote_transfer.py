import math
import os
import signal
import unittest
import time
import random
from typing import Optional, Tuple, Any

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.ray_utils import get_head_node_id
from volga.streaming.common.stats import create_streaming_stats_manager, aggregate_streaming_historical_stats
from volga.streaming.runtime.network.channel import RemoteChannel
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
from volga.streaming.runtime.network.network_config import DEFAULT_DATA_WRITER_CONFIG, DataWriterConfig, \
    DEFAULT_DATA_READER_CONFIG
from volga.streaming.runtime.network.remote.transfer_actor import TransferActor
from volga.streaming.runtime.network.remote.transfer_io_handlers import TransferSender, TransferReceiver
from volga.streaming.runtime.network.testing_utils import TestWriter, TestReader, start_ray_io_handler_actors, \
    StatsActor


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
        run_for_s = 10
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
        stats_manager = create_streaming_stats_manager()
        for reader in readers:
            stats_manager.register_target(reader)

        time.sleep(1)
        stats_manager.start()
        futs = []
        start_ts = time.time()
        for i in range(n):
            futs.append(writers[i].round_robin_send.remote([channels[i].channel_id], msg_size, run_for_s))

        for i in range(n):
            futs.append(readers[i].receive_items.remote(run_for_s + 120))

        res = ray.get(futs)
        num_msgs_sent_total = {}
        num_msgs_rcvd_total = {}
        i = 0
        for _ in range(n):
            num_msgs_sent = res[i]
            for channel_id in num_msgs_sent:
                if channel_id in num_msgs_sent_total:
                    num_msgs_sent_total[channel_id] += num_msgs_sent[channel_id]
                else:
                    num_msgs_sent_total[channel_id] = num_msgs_sent[channel_id]
            i += 1

        for _ in range(n):
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

        historical_stats = stats_manager.get_historical_stats()
        avg_throughput, latency_stats, _, _ = aggregate_streaming_historical_stats(historical_stats)
        stats_manager.stop()

        t = time.time() - start_ts
        estimated_throughput = num_msgs/t
        print(f'Finished in {t}s \n'
              f'Avg Throughput: {avg_throughput} msg/s \n'
              f'Estimated Throughput: {estimated_throughput} msg/s \n'
              f'Latency: {latency_stats} \n')
        time.sleep(1)

        stop_futs = [r.stop.remote() for r in readers] + [w.stop.remote() for w in writers] + [source_transfer_actor.stop.remote(), target_transfer_actor.stop.remote()]
        ray.get(stop_futs)
        ray.shutdown()

    # TODO add latency sampling rate variable
    # reader/writer + transfer per node, star topology (nw*nr)
    def test_nw_to_nr_star_on_ray(
        self,
        nw: int,
        nr: int,
        msg_size: int = 32,
        batch_size: int = 10,
        run_for_s: int = 30,
        num_workers_per_node: Optional[int] = None,
        ray_addr: Optional[str] = None,
        runtime_env: Optional[Any] = None,
        multinode: bool = False
    ) -> Tuple:
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

        stats_actor_options = {'num_cpus': 0}
        stats_actor_options['scheduling_strategy'] = NodeAffinitySchedulingStrategy(
            node_id=get_head_node_id(),
            soft=False
        )
        stats_actor = StatsActor.options(**stats_actor_options).remote([readers[reader_id] for reader_id in readers])
        ray.get(stats_actor.start.remote())

        start_ts = time.time()
        futs = []
        for writer_id in writers:
            futs.append(writers[writer_id].round_robin_send.remote([channel.channel_id for channel in writer_channels[writer_id]], msg_size, run_for_s))

        for reader_id in readers:
            futs.append(readers[reader_id].receive_items.remote(run_for_s + 120))

        res = ray.get(futs)
        num_msgs_sent_total = {}
        num_msgs_rcvd_total = {}
        i = 0
        for _ in range(len(writers)):
            num_msgs_sent = res[i]
            for channel_id in num_msgs_sent:
                if channel_id in num_msgs_sent_total:
                    num_msgs_sent_total[channel_id] += num_msgs_sent[channel_id]
                else:
                    num_msgs_sent_total[channel_id] = num_msgs_sent[channel_id]
            i += 1

        for _ in range(len(readers)):
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

        ray.get(stats_actor.stop.remote())
        historical_stats = ray.get(stats_actor.get_historical_stats.remote())

        avg_throughput, latency_stats, hist_throughput, hist_latency = aggregate_streaming_historical_stats(historical_stats)

        run_duration = time.time() - start_ts
        estimated_throughput = num_msgs / run_duration

        print(f'Finished in {run_duration}s \n'
              f'Avg Throughput: {avg_throughput} msg/s \n'
              f'Estimated Throughput: {estimated_throughput} msg/s \n'
              f'Latency: {latency_stats} \n')

        stop_futs = [actor.stop.remote() for actor in actors]
        ray.get(stop_futs)

        ray.shutdown()
        return avg_throughput, latency_stats, num_msgs, hist_throughput, hist_latency

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
        while not ray.get(readers[0].all_done.remote()):
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
            data_reader.read_message_batch()
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


if __name__ == '__main__':
    t = TestRemoteTransfer()
    # t.test_n_to_n_parallel_on_ray(n=4)
    t.test_nw_to_nr_star_on_ray(nr=1, nw=1, msg_size=32, batch_size=1000, run_for_s=30)
    # t.test_nw_to_nr_star_on_ray(nr=4, nw=4, msg_size=32, batch_size=1000, run_for_s=30, num_workers_per_node=4, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, multinode=True)
