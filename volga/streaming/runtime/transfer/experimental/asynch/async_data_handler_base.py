import asyncio
from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict

import zmq.asyncio as zmq_async

from volga.streaming.runtime.transfer.buffer_pool import BufferPool
from volga.streaming.runtime.transfer.experimental.channel import BiChannel


# Bi-directional connection data handler, sends and receives messages, acts as a base for DataReader/DataWriter
# Each Channel instance has a corresponding pair of sockets:
# zmq.PULL socket in _rcv_sockets and zmq.PUSH in _send_sockets
class AsyncDataHandlerBase(ABC):

    def __init__(
        self,
        name: str,
        channels: List[BiChannel],
        node_id: str,
        zmq_ctx: zmq_async.Context
    ):
        self.name = name
        self.running = False
        self._sender_event_loop = None
        self._receiver_event_loop = None
        self._sender_thread = Thread(target=self._start_sender_event_loop)
        self._receiver_thread = Thread(target=self._start_receiver_event_loop)
        self._zmq_ctx = zmq_ctx

        self._channels = channels
        self._channel_map = {c.channel_id: c for c in self._channels}

        self._send_sockets: Dict[str, zmq_async.Socket] = {}
        self._rcv_sockets: Dict[zmq_async.Socket, str] = {}
        self._rcv_poller = zmq_async.Poller()

        self._buffer_pool = BufferPool.instance(node_id=node_id)

    # send and rcv sockets are inited in different threads, hence separation
    @abstractmethod
    def _init_send_sockets(self):
        raise NotImplementedError()

    @abstractmethod
    def _init_rcv_sockets(self):
        raise NotImplementedError()

    def _start_sender_event_loop(self):
        self._sender_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._sender_event_loop)
        self._init_send_sockets()
        self._sender_event_loop.run_forever()

    def _start_receiver_event_loop(self):
        self._receiver_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._receiver_event_loop)
        self._init_rcv_sockets()
        self._receiver_event_loop.run_forever()

    async def _close_send_sockets(self):
        await asyncio.gather(*[self._send_sockets[c.channel_id].close(linger=0) for c in self._channels])

    async def _close_rcv_sockets(self):
        await asyncio.gather(*[s.close(linger=0) for s in self._rcv_sockets])

    def start(self):
        self.running = True
        self._sender_thread.start()
        self._receiver_thread.start()

    def close(self):
        self.running = False
        asyncio.run_coroutine_threadsafe(self._close_send_sockets(), self._sender_event_loop)
        asyncio.run_coroutine_threadsafe(self._close_rcv_sockets(), self._receiver_event_loop)
        # TODO call loop stop only after _close_sockets was awaited
        self._sender_event_loop.call_soon_threadsafe(self._sender_event_loop.stop)
        self._receiver_event_loop.call_soon_threadsafe(self._receiver_event_loop.stop)
        self._sender_thread.join(timeout=5)
        self._receiver_thread.join(timeout=5)