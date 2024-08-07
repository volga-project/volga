from typing import List, Dict, Any

from volga.streaming.runtime.network_rust.channel import Channel
from volga.streaming.runtime.network_rust.io_loop import IOLoop
from volga.streaming.runtime.network_rust.local.data_reader import DataReader
from volga.streaming.runtime.network_rust.local.data_writer import DataWriter
import time
import ray

@ray.remote(max_concurrency=4)
class TestWriter:
    def __init__(
        self,
        job_name: str,
        channel: Channel,
        batch_size: int = 1000,
        delay_s: float = 0,
    ):
        self.channel = channel
        self.io_loop = IOLoop('writer_loop')
        self.delay_s = delay_s
        self.data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            job_name=job_name,
            channels=[channel],
            batch_size=batch_size
        )
        self.io_loop.register_io_handler(self.data_writer)

    def start(self, num_threads: int = 1):
        return self.io_loop.start(num_threads)

    def send_items(self, items: List[Dict]):
        i = 0
        start_ts = time.time()
        last_report = time.time()
        for item in items:
            succ = self.data_writer.try_write_message(self.channel.channel_id, item)
            t = time.time()
            while not succ:
                if time.time() - t > 10:
                    raise RuntimeError('Timeout writing data')
                time.sleep(0.0001)
                succ = self.data_writer.try_write_message(self.channel.channel_id, item)
            i += 1
            # Report every second
            if time.time() - last_report > 1 or i == len(items):
                tx = i/(time.time() - start_ts)
                print(f'Sent {i} msgs, tx {tx} msg/s')
                last_report = time.time()
            if self.delay_s > 0:
                time.sleep(self.delay_s)

    def close(self):
        self.io_loop.close()


@ray.remote(max_concurrency=4)
class TestReader:
    def __init__(
        self,
        job_name: str,
        channel: Channel,
        num_expected: int,
    ):
        self.channel = channel
        self.io_loop = IOLoop('reader_loop')
        self.data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            job_name=job_name,
        )
        self.num_expected = num_expected
        self.io_loop.register_io_handler(self.data_reader)
        self.res = []

    def start(self, num_threads: int = 1):
        return self.io_loop.start(num_threads)

    def receive_items(self) -> List[Any]:
        i = 0
        last_report = time.time()
        start_ts = time.time()
        while True:
            if time.time() - start_ts > 3000:
                raise RuntimeError('Timeout reading data')

            items = self.data_reader.read_message()
            if items is None:
                time.sleep(0.001)
                continue

            i += len(items)
            # Report every second
            if time.time() - last_report > 1:
                rx = i/(time.time() - start_ts)
                print(f'Recvd {i} msgs, rx: {rx} msg/s')
                last_report = time.time()
            self.res.extend(items)
            if len(self.res) == self.num_expected:
                print(f'Recvd {i}')
                break
        return self.res

    def get_items(self) -> List:
        return self.res

    def close(self):
        self.io_loop.close()


def start_ray_io_handler_actors(handler_actors: List):
    futs = [h.start.remote() for h in handler_actors]
    ray.get(futs)