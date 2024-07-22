import time
from collections import deque
from random import randrange, choices
from threading import Thread
from typing import List, Any, Dict, Tuple

import zmq

from volga.streaming.runtime.network.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network.buffer.buffering_policy import BufferPerMessagePolicy, BufferingPolicy
from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.transfer.io_loop import IOLoop, IOHandler
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
import ray

import volga

REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV = {
    'pip': [
        'pydantic==1.10.13',
        'simplejson==3.19.2',
        'orjson==3.10.6',
        'pyzmq==23.2.0',
        'shared-memory-dict==0.7.2',
        'locket==1.0.0',
        'aenum==3.1.15'
    ],
    'py_modules': [volga]
}


@ray.remote(max_concurrency=4)
class TestWriter:
    def __init__(
        self,
        job_name: str,
        channel: Channel,
        delay_s: float = 0,
        buffering_policy: BufferingPolicy = BufferPerMessagePolicy(),
        buffering_config: BufferingConfig = BufferingConfig()
    ):
        self.channel = channel
        self.io_loop = IOLoop()
        self.delay_s = delay_s
        self.data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            job_name=job_name,
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance(),
            buffering_policy=buffering_policy,
            buffering_config=buffering_config
        )
        self.io_loop.register(self.data_writer)

    def start(self) -> Tuple[bool, Any]:
        return self.io_loop.start()

    def send_items(self, items: List[Dict]):
        for item in items:
            succ = self.data_writer._write_message(self.channel.channel_id, item, timeout=10)
            if not succ:
                raise RuntimeError('Unable to send message after backpressure timeout')
            if self.delay_s > 0:
                time.sleep(self.delay_s)

    def get_stats(self):
        return self.data_writer.stats

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
        self.io_loop = IOLoop()
        self.data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            job_name=job_name,
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        self.num_expected = num_expected
        self.io_loop.register(self.data_reader)
        self.res = []

    def start(self) -> Tuple[bool, Any]:
        return self.io_loop.start()

    def receive_items(self) -> List[Any]:
        t = time.time()
        while True:
            if time.time() - t > 3000:
                raise RuntimeError('Timeout reading data')

            items = self.data_reader.read_message()
            if len(items) == 0:
                time.sleep(0.001)
                continue
            self.res.extend(items)
            if len(self.res) == self.num_expected:
                break
        return self.res

    def get_items(self) -> List:
        return self.res

    def get_stats(self):
        return self.data_reader.stats

    def close(self):
        self.io_loop.close()


# local utils
def read(rcvd: List, data_reader: DataReader, num_expected: int):
    t = time.time()
    while True:
        if time.time() - t > 180:
            raise RuntimeError('Timeout reading data')
        msgs = data_reader.read_message()
        # time.sleep(0.01)
        if len(msgs) == 0:
            time.sleep(0.001)
            continue
        else:
            rcvd.extend(msgs)

        if len(rcvd) == num_expected:
            break


def write(to_send: List, data_writer: DataWriter, channel: Channel):
    for msg in to_send:
        succ = data_writer._write_message(channel.channel_id, msg, timeout=10)
        if not succ:
            raise RuntimeError('Unable to send message after backpressure timeout')
        # time.sleep(0.2)


class FakeSocket:

    def __init__(self, name: str, in_queue: deque, out_queue: deque, is_writer: bool, loss_rate: float, exception_rate: float, out_of_orderness: float):
        self.name = name
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.is_writer = is_writer
        self.loss_rate = loss_rate
        self.exception_rate = exception_rate
        self.out_of_orderness = out_of_orderness

    def _raise_exc_prob(self):
        if choices([0, 1], weights=[self.exception_rate, 1 - self.exception_rate])[0] == 0:
            raise RuntimeError('FakeException')

    def send(self, data, flags=None):
        self._raise_exc_prob()
        if choices([0, 1], weights=[self.loss_rate, 1 - self.loss_rate])[0] == 0:
            # lost message
            return
        if self.is_writer:
            insert_random(self.out_queue, data, self.out_of_orderness)
        else:
            insert_random(self.in_queue, data, self.out_of_orderness)

    def recv(self, flags=None):
        self._raise_exc_prob()
        if self.is_writer:
            return self.in_queue.popleft()
        else:
            return self.out_queue.popleft()

    def poll(self, send: bool) -> bool:
        if send:
            # TODO implement limits on send
            return True
        if self.is_writer:
            return len(self.in_queue) != 0
        else:
            return len(self.out_queue) != 0

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name


class FakeIOLoop:

    def __init__(self, data_reader: DataReader, data_writer: DataWriter, loss_rate: float, exception_rate: float, out_of_orderness: float):
        assert len(data_reader._channels) == len(data_writer._channels)
        self.running = False
        self._thread = Thread(target=self._loop)
        self._socket_to_handler = {}
        self._in_queues = {}
        self._out_queues = {}
        data_reader.io_loop = self
        data_writer.io_loop = self
        for c in data_writer._channels:
            self._in_queues[c.channel_id] = deque()
            self._out_queues[c.channel_id] = deque()
            writer_socket = FakeSocket(f'writer_{c.channel_id}', self._in_queues[c.channel_id],
                                       self._out_queues[c.channel_id], True, loss_rate, exception_rate, out_of_orderness)
            reader_socket = FakeSocket(f'reader_{c.channel_id}', self._in_queues[c.channel_id],
                                       self._out_queues[c.channel_id], False, loss_rate, exception_rate, out_of_orderness)
            data_reader._ch_to_socket[c.channel_id] = reader_socket
            data_reader._socket_to_ch[reader_socket] = c.channel_id

            data_writer._ch_to_socket[c.channel_id] = writer_socket
            data_writer._socket_to_ch[writer_socket] = c.channel_id

            self._socket_to_handler[reader_socket] = data_reader
            self._socket_to_handler[writer_socket] = data_writer

    def _loop(self):
        while self.running:
            for socket in self._socket_to_handler:
                handler = self._socket_to_handler[socket]
                assert isinstance(socket, FakeSocket)
                if socket.poll(send=True):
                    handler.send(socket)
                if socket.poll(send=False):
                    handler.rcv(socket)

    def start(self):
        self.running = True
        self._thread.start()

    def close(self):
        self.running = False
        self._thread.join(timeout=5)


def insert_random(q: deque, el: Any, out_of_orderness: float):
    if out_of_orderness == 0:
        q.append(el)
        return
    l = len(q)
    if l <= 1:
        q.append(el)
        return
    i = randrange(int((1-out_of_orderness) * l), l)
    q.insert(i, el)

def start_ray_io_handler_actors(handler_actors: List):
    futs = [h.start.remote() for h in handler_actors]
    res = ray.get(futs)
    for i in range(len(res)):
        succ, err = res[i]
        if not succ:
            raise RuntimeError(f'Failed to start {handler_actors[i].__class__.__name__}, err: {err}')

