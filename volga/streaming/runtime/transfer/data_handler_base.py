from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict, Optional

import zmq

from volga.streaming.runtime.transfer.channel import Channel, LocalChannel, RemoteChannel
import zmq.asyncio as zmq_async

from volga.streaming.runtime.transfer.buffer_pool import BufferPool
from volga.streaming.runtime.transfer.config import ZMQConfig


# Bidirectional connection data handler, sends and receives messages, acts as a base for DataReader/DataWriter
# Each Channel instance has a represents a zmq.PAIR socket
class DataHandlerBase(ABC):

    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq_async.Context,
        is_reader: bool, # we should find more elegant way of detecting subclass type
        zmq_config: Optional[ZMQConfig]
    ):
        self._is_reader = is_reader
        self.name = name
        self.running = False
        self._thread = Thread(target=self._start_loop)
        self._zmq_ctx = zmq_ctx
        self._zmq_config = zmq_config

        self._channels = channels
        self._channel_map = {c.channel_id: c for c in self._channels}

        self._ch_to_socket: Dict[str, zmq.Socket] = {}
        self._socket_to_ch: Dict[zmq.Socket, str] = {}
        self._poller = zmq.Poller()

        self._buffer_pool = BufferPool.instance(node_id=node_id)

    def _init_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._ch_to_socket:
                raise RuntimeError('duplicate channel ids')

            socket = self._zmq_ctx.socket(zmq.PAIR)

            # configure
            if self._zmq_config is not None:
                if self._zmq_config.LINGER is not None:
                    socket.setsockopt(zmq.LINGER, self._zmq_config.LINGER)
                if self._zmq_config.SNDHWM is not None:
                    socket.setsockopt(zmq.SNDHWM, self._zmq_config.SNDHWM)
                if self._zmq_config.RCVHWM is not None:
                    socket.setsockopt(zmq.RCVHWM, self._zmq_config.RCVHWM)
                if self._zmq_config.SNDBUF is not None:
                    socket.setsockopt(zmq.SNDBUF, self._zmq_config.SNDBUF)
                if self._zmq_config.RCVBUF is not None:
                    socket.setsockopt(zmq.RCVBUF, self._zmq_config.RCVBUF)

            if isinstance(channel, LocalChannel):
                if self._is_reader:
                    socket.connect(channel.ipc_addr)
                else:
                    socket.bind(channel.ipc_addr)
            elif isinstance(channel, RemoteChannel):
                raise ValueError('RemoteChannel not supported yet')
            else:
                raise ValueError('Unknown channel type')
            self._ch_to_socket[channel.channel_id] = socket
            self._socket_to_ch[socket] = channel.channel_id
            self._poller.register(socket, zmq.POLLIN | zmq.POLLOUT)

    @abstractmethod
    def _rcv(self, channel_id: str, socket: zmq.Socket):
        raise NotImplementedError()

    @abstractmethod
    def _send(self, channel_id: str, socket: zmq.Socket):
        raise NotImplementedError()

    def _loop(self):
        while self.running:
            sockets_and_flags = self._poller.poll()
            for (socket, flag) in sockets_and_flags:
                channel_id = self._socket_to_ch[socket]
                if flag == zmq.POLLIN:
                    self._rcv(channel_id, socket)
                elif flag == zmq.POLLOUT:
                    self._send(channel_id, socket)
                elif flag == zmq.POLLOUT | zmq.POLLIN:
                    self._send(channel_id, socket)
                    self._rcv(channel_id, socket)
                else:
                    raise RuntimeError(f'Unknown flag {flag}')

    def _start_loop(self):
        self._init_sockets()
        self._loop()
        self._close_sockets()

    def _close_sockets(self):
        for c in self._channels:
            self._ch_to_socket[c.channel_id].close(linger=0)

    def start(self):
        self.running = True
        self._thread.start()

    def close(self):
        self.running = False
        self._thread.join(timeout=5)