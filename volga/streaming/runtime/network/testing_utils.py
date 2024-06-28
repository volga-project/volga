import time
from collections import deque
from random import randrange, choices
from threading import Thread
from typing import List, Any, Dict

import zmq

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.transfer.io_loop import IOLoop
from volga.streaming.runtime.network.transfer.local.data_reader import DataReader
from volga.streaming.runtime.network.transfer.local.data_writer import DataWriter
import ray

import volga

REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV = {
    'pip': [
        'pydantic==1.10.13',
        'simplejson==3.19.2',
        'pyzmq==23.2.0',
        'shared-memory-dict==0.7.2',
        'lockit==1.0.1',
        'aenum==3.1.15'
    ],
    'py_modules': [volga]
}


@ray.remote
class TestWriter:
    def __init__(
            self,
            channel: Channel,
    ):
        self.channel = channel
        self.io_loop = IOLoop()
        self.data_writer = DataWriter(
            name='test_writer',
            source_stream_name='0',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        self.io_loop.register(self.data_writer)
        self.io_loop.start()
        print('writer inited')

    def send_items(self, items: List[Dict]):
        for item in items:
            self.data_writer._write_message(self.channel.channel_id, item)

    def get_stats(self):
        return self.data_writer.stats

    def close(self):
        self.io_loop.close()


@ray.remote
class TestReader:
    def __init__(
            self,
            channel: Channel,
            num_expected: int,
    ):
        self.channel = channel
        self.io_loop = IOLoop()
        self.data_reader = DataReader(
            name='test_reader',
            channels=[channel],
            node_id='0',
            zmq_ctx=zmq.Context.instance()
        )
        self.num_expected = num_expected
        self.io_loop.register(self.data_reader)
        self.io_loop.start()
        print('reader inited')

    def receive_items(self) -> List[Any]:
        t = time.time()
        res = []
        while True:
            if time.time() - t > 300:
                raise RuntimeError('Timeout reading data')

            items = self.data_reader.read_message()
            if len(items) == 0:
                time.sleep(0.001)
                continue
            res.extend(items)
            if len(res) == self.num_expected:
                break
        return res

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
        if len(msgs) == 0:
            time.sleep(0.001)
            continue
        else:
            # print(f'Read: {msg}')
            rcvd.extend(msgs)

        if len(rcvd) == num_expected:
            break


def write(to_send: List, data_writer: DataWriter, channel: Channel):
    for msg in to_send:
        data_writer._write_message(channel.channel_id, msg)
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
