from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict

import zmq

from volga.streaming.runtime.network_deprecated.channel import Channel
import zmq.asyncio as zmq_async

from volga.streaming.runtime.network_deprecated.buffer.buffer_memory_tracker import BufferMemoryTracker


# Bi-directional connection data handler, sends and receives messages, acts as a base for DataReader/DataWriter
# Each Channel instance has a corresponding pair of sockets:
# zmq.PULL socket in _rcv_sockets and zmq.PUSH in _send_sockets
class DataHandlerBase(ABC):

    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq_async.Context
    ):
        self.name = name
        self.running = False
        self._sender_thread = Thread(target=self._start_sender_loop)
        self._receiver_thread = Thread(target=self._start_receiver_loop)
        self._zmq_ctx = zmq_ctx

        self._channels = channels
        self._channel_map = {c.channel_id: c for c in self._channels}

        self._send_sockets: Dict[str, zmq.Socket] = {}
        self._rcv_sockets: Dict[zmq.Socket, str] = {}
        self._rcv_poller = zmq.Poller()

        self._buffer_pool = BufferMemoryTracker.instance(node_id=node_id)

    # send and rcv sockets are inited in different threads, hence separation
    @abstractmethod
    def _init_send_sockets(self):
        raise NotImplementedError()

    @abstractmethod
    def _init_rcv_sockets(self):
        raise NotImplementedError()

    @abstractmethod
    def _send_loop(self):
        raise NotImplementedError()

    @abstractmethod
    def _receive_loop(self):
        raise NotImplementedError()

    def _start_sender_loop(self):
        self._init_send_sockets()
        self._send_loop()
        self._close_send_sockets()

    def _start_receiver_loop(self):
        self._init_rcv_sockets()
        self._receive_loop()
        self._close_rcv_sockets()

    def _close_send_sockets(self):
        for c in self._channels:
            self._send_sockets[c.channel_id].close(linger=0)

    def _close_rcv_sockets(self):
        for s in self._rcv_sockets:
            s.close(linger=0)

    def start(self):
        self.running = True
        self._sender_thread.start()
        self._receiver_thread.start()

    def close(self):
        self.running = False
        self._sender_thread.join(timeout=5)
        self._receiver_thread.join(timeout=5)