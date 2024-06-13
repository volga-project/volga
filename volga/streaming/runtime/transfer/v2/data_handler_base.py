import asyncio
from threading import Thread
from typing import List, Dict

import zmq

from volga.streaming.runtime.transfer.channel import Channel, LocalChannel, RemoteChannel
import zmq.asyncio as zmq_async

from volga.streaming.runtime.transfer.v2.buffer_pool import BufferPool


# Bi-directional connection data handler, sends and receives messages, acts as a base for DataReader/DataWriter
# Each Channel instance has a corresponding pair of sockets:
# zmq.PULL socket in _rcv_sockets and zmq.PUSH in _send_sockets
class DataHandlerBase:

    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq_async.Context,
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
        self._rcv_poller = None

        self._buffer_pool = BufferPool.instance(node_id=node_id)

    # send and rcv sockets are inited in different threads, hence separation
    def _init_send_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._send_sockets:
                raise RuntimeError('duplicate channel ids for send')

            # TODO set HWM
            send_socket = self._zmq_ctx.socket(zmq.PUSH)
            send_socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalChannel):
                send_socket.bind(channel.ipc_addr_to)
            elif isinstance(channel, RemoteChannel):
                send_socket.bind(channel.source_local_ipc_addr_to)
            else:
                raise ValueError('Unknown channel type')
            self._send_sockets[channel.channel_id] = send_socket

    def _init_rcv_sockets(self):
        self._rcv_poller = zmq_async.Poller()
        for channel in self._channels:
            if channel.channel_id in self._rcv_sockets:
                raise RuntimeError('duplicate channel ids for rcv')

            # TODO set HWM
            rcv_socket = self._zmq_ctx.socket(zmq.PULL)
            rcv_socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalChannel):
                rcv_socket.connect(channel.ipc_addr_from)
            elif isinstance(channel, RemoteChannel):
                rcv_socket.connect(channel.source_local_ipc_addr_from)
            else:
                raise ValueError('Unknown channel type')
            self._rcv_sockets[rcv_socket] = channel.channel_id
            self._rcv_poller.register(rcv_socket, zmq.POLLIN)

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