from typing import List, Dict, Any, Optional

from volga.streaming.common.utils import ms_to_s, now_ts_ms
from volga.streaming.runtime.master.stats.stats_manager import WorkerLatencyStatsState, WorkerThroughputStatsState, \
    WorkerStatsUpdate
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
        'aenum==3.1.15'
    ],
    'py_modules': [
        volga,
        # '/Users/anov/Desktop/volga-rust-builds/volga_rust-0.1.0-cp310-cp310-manylinux_2_35_x86_64.whl'
        '/Users/anov/IdeaProjects/volga/rust/target/wheels/volga_rust-0.1.0-cp310-cp310-manylinux_2_17_aarch64.manylinux2014_aarch64.whl'
    ]
}


@ray.remote(max_concurrency=4)
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

    def send_items(self, num_items_per_channel: Dict[str, int], msg_size: int):
        index = {channel_id: 0 for channel_id in num_items_per_channel}
        channel_ids = list(num_items_per_channel.keys())
        cur_channel_index = 0
        num_sent = 0
        num_to_send = sum(list(num_items_per_channel.values()))
        # round-robin send
        while num_sent != num_to_send:
            channel_id = channel_ids[cur_channel_index]
            num_items = num_items_per_channel[channel_id]
            i = index[channel_id]
            if i == num_items:
                cur_channel_index = (cur_channel_index + 1)%len(channel_ids)
                continue

            mark_latency = False
            if num_sent % 100 == 0:
                mark_latency = True

            item = construct_message(i, msg_size, mark_latency)
            succ = self.data_writer.try_write_message(channel_id, item)
            if succ:
                index[channel_id] += 1
                num_sent += 1
            cur_channel_index = (cur_channel_index + 1) % len(channel_ids)

    def get_name(self):
        return self.name

    def stop(self):
        self.io_loop.stop()


@ray.remote(max_concurrency=4)
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
        self.num_rcvd = 0

        # stats

        self.latency_stats = WorkerLatencyStatsState.create()
        self.throughput_stats = WorkerThroughputStatsState.create()

    def start(self) -> Optional[str]:
        return self.io_loop.connect_and_start()

    def receive_items(self, num_expected) -> bool:
        start_ts = time.time()
        while True:
            if time.time() - start_ts > 3000:
                raise RuntimeError('Timeout reading data')

            items = self.data_reader.read_message()
            if items is None:
                time.sleep(0.001)
                continue

            for item in items:
                if 'emit_ts' in item:
                    ts = now_ts_ms()
                    latency = ts - item['emit_ts']
                    self.latency_stats.observe(latency, ts)

            self.throughput_stats.inc(len(items))
            self.num_rcvd += len(items)

            if self.num_rcvd == num_expected:
                break
        return self.num_rcvd == num_expected

    def get_num_rcvd(self) -> int:
        return self.num_rcvd

    def collect_stats(self) -> List[WorkerStatsUpdate]:
        return [self.throughput_stats.collect(), self.latency_stats.collect()]

    def get_name(self):
        return self.name

    def stop(self):
        self.io_loop.stop()


def construct_message(i: int, msg_size: int, mark_latency: bool) -> Dict:
    msg = {'k': i, 'v': 'a' * msg_size}
    if mark_latency:
        msg['emit_ts'] = now_ts_ms()
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

