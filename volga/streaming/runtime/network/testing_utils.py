from typing import List, Dict, Any, Optional, Tuple

from volga.streaming.common.utils import ms_to_s, now_ts_ms
from volga.streaming.runtime.master.stats.stats_manager import WorkerLatencyStatsState, WorkerThroughputStatsState, \
    WorkerStatsUpdate, StatsManager
from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.io_loop import IOLoop
from volga.streaming.runtime.network.local.data_reader import DataReader
from volga.streaming.runtime.network.local.data_writer import DataWriter
import time
import volga
import ray
from volga.streaming.runtime.network.network_config import DataWriterConfig, DEFAULT_DATA_WRITER_CONFIG

RAY_ADDR = 'ray://127.0.0.1:12345'
# RAY_ADDR = 'ray://ray-cluster-kuberay-head-svc:10001'
REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV = {
    'pip': [
        'pydantic==1.10.13',
        'simplejson==3.19.2',
        'orjson==3.10.6',
        'aenum==3.1.15',
        'sortedcontainers==2.4.0'
    ],
    'py_modules': [
        volga,
        '/Users/anov/Desktop/volga-rust-builds/volga_rust-0.1.0-cp310-cp310-manylinux_2_35_x86_64.whl'
        # '/Users/anov/IdeaProjects/volga/rust/target/wheels/volga_rust-0.1.0-cp310-cp310-manylinux_2_17_aarch64.manylinux2014_aarch64.whl'
    ]
}


@ray.remote(max_concurrency=999)
class TestWriter:
    def __init__(
        self,
        writer_id: int,
        job_name: str,
        channels: List[Channel],
        writer_config: DataWriterConfig = DEFAULT_DATA_WRITER_CONFIG,
    ):
        self.name = f'test_writer_{writer_id}'
        self.channels = channels
        self.io_loop = IOLoop(f'writer_loop_{writer_id}')
        self.data_writer = DataWriter(
            handler_id=str(writer_id),
            name=self.name,
            source_stream_name='0',
            job_name=job_name,
            channels=channels,
            config=writer_config
        )
        self.io_loop.register_io_handler(self.data_writer)

    def start(self) -> Optional[str]:
        return self.io_loop.connect_and_start()

    # round robins messages to channels for timeout_s.
    # returns dict containing number of sent messages per channel
    def round_robin_send(self, channel_ids: List[str], msg_size: int, timeout_s: int, mark_latency_every: int = 100) -> Dict[str, int]:
        num_sent_per_channel = {channel_id: 0 for channel_id in channel_ids}
        cur_channel_index = 0
        # round-robin send
        start_ts = time.time()
        while time.time() - start_ts <= timeout_s:
            channel_id = channel_ids[cur_channel_index]
            num_sent = num_sent_per_channel[channel_id]
            mark_latency = False
            if num_sent % mark_latency_every == 0:
                mark_latency = True

            msg = construct_message(num_sent, channel_id, msg_size, mark_latency)
            succ = self.data_writer.try_write_message(channel_id, msg)
            if succ:
                num_sent_per_channel[channel_id] += 1
            cur_channel_index = (cur_channel_index + 1) % len(channel_ids)

        # send terminal messages to all channels
        for channel_id in channel_ids:
            terminal_msg = construct_message(num_sent_per_channel[channel_id], channel_id, msg_size, False, True)
            ts = time.time()
            while not self.data_writer.try_write_message(channel_id, terminal_msg):
                time.sleep(0.1)
                if time.time() - ts > 5:
                    raise RuntimeError(f'Timeout sending terminal message to channel {channel_id}')

        return num_sent_per_channel

    def get_name(self):
        return self.name

    def stop(self):
        self.io_loop.stop()


@ray.remote(max_concurrency=999)
class TestReader:
    def __init__(
        self,
        reader_id: int,
        job_name: str,
        channels: List[Channel]
    ):
        self.name=f'test_reader_{reader_id}'
        self.channels = channels
        self.io_loop = IOLoop(f'reader_loop_{reader_id}')
        self.data_reader = DataReader(
            handler_id=str(reader_id),
            name=self.name,
            channels=channels,
            job_name=job_name,
        )
        self.io_loop.register_io_handler(self.data_reader)
        self.terminal_messages_received =  {channel.channel_id: False for channel in self.channels}

        # stats
        self.latency_stats = WorkerLatencyStatsState.create()
        self.throughput_stats = WorkerThroughputStatsState.create()

    def start(self) -> Optional[str]:
        return self.io_loop.connect_and_start()

    def receive_items(self, timeout_s) -> Dict[str, int]:
        num_rcvd_per_channel = {channel.channel_id: 0 for channel in self.channels}
        start_ts = time.time()
        while time.time() - start_ts <= timeout_s:
            msgs = self.data_reader.read_message()
            if msgs is None:
                time.sleep(0.001)
                continue

            for msg in msgs:
                channel_id = msg['channel_id']
                if 'emit_ts' in msg:
                    ts = now_ts_ms()
                    latency = ts - msg['emit_ts']
                    self.latency_stats.observe(latency, ts)
                if 'is_terminal' in msg:
                    if self.terminal_messages_received[channel_id]:
                        raise RuntimeError(f'Duplicate terminal message for channel {channel_id}')
                    self.terminal_messages_received[channel_id] = True
                else:
                    num_rcvd_per_channel[channel_id] += 1
                if self.all_done():
                    return num_rcvd_per_channel

            self.throughput_stats.inc(len(msgs))

        raise RuntimeError('Timeout reading data')

    def all_done(self) -> bool:
        for channel_id in self.terminal_messages_received:
            if not self.terminal_messages_received[channel_id]:
                return False

        return True

    def collect_stats(self) -> List[WorkerStatsUpdate]:
        return [self.throughput_stats.collect(), self.latency_stats.collect()]

    def get_name(self):
        return self.name

    def stop(self):
        self.io_loop.stop()


@ray.remote(max_concurrency=999)
class StatsActor:

    def __init__(self, readers: List):
        self.stats_manager = StatsManager()
        for reader in readers:
            self.stats_manager.register_worker(reader)

    def start(self):
        self.stats_manager.start()

    def stop(self):
        self.stats_manager.stop()

    def get_final_aggregated_stats(self) -> Tuple[float, Dict[str, float]]:
        return self.stats_manager.get_final_aggregated_stats()



def construct_message(msg_id: int, channel_id: str, msg_size: int, mark_latency: bool, is_terminal: bool = False) -> Dict:
    msg = {'msg_id': msg_id, 'channel_id': channel_id, 'payload': 'a' * msg_size}
    if mark_latency:
        msg['emit_ts'] = now_ts_ms()
    if is_terminal:
        msg['is_terminal'] = True
    return msg


def start_ray_io_handler_actors(handler_actors: List):
    name_futs = [h.get_name.remote() for h in handler_actors]
    names = ray.get(name_futs)

    # TODO there is some sort of race condition - if we delete above name_futs getting some actors fail to bind/connect
    # TODO maybe we need to give them a certain timeout to "warm-up"?
    futs = [h.start.remote() for h in handler_actors]
    res = ray.get(futs)
    errs = {}
    for i in range(len(res)):
        err = res[i]
        name = names[i]
        if err is not None:
            errs[name] = err
    if len(errs) != 0:
        raise RuntimeError(f'Failed to start: {errs}')
