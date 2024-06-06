import asyncio
import logging
from collections import deque
from threading import Thread
from typing import List, Optional, Dict

from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage, RemoteChannel, LocalChannel

import zmq
import zmq.asyncio as zmq_async
import simplejson

from volga.streaming.runtime.transfer.v2.buffer_pool import BufferPool

logger = logging.getLogger("ray")


class DataReaderV2:
    def __init__(
        self,
        name: str,
        input_channels: List[Channel],
    ):

        self.name = name
        self._in_channels = input_channels
        self._channel_map = {c.channel_id: c for c in self._in_channels}
        self.cur_read_id = 0
        self.running = True

        self._reader_event_loop = asyncio.new_event_loop()
        self._reader_thread = Thread(target=self._start_reader_event_loop)

        self._buffer_pools: Dict[str, BufferPool] = {c.channel_id: BufferPool() for c in self._in_channels}
        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in self._in_channels}

        self._zmq_ctx = None
        self._poller = None
        self._in_sockets = {}

    def _init_sockets(self):
        self._zmq_ctx = zmq_async.Context.instance(io_threads=64)  # TODO configure
        self._poller = zmq_async.Poller()
        for channel in self._in_channels:
            if channel.channel_id in self._in_sockets:
                raise RuntimeError('duplicate channel ids')

            # TODO set HWM
            socket = self._zmq_ctx.socket(zmq.PULL)
            socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalChannel):
                socket.connect(channel.ipc_addr)
            elif isinstance(channel, RemoteChannel):
                socket.connect(channel.target_local_ipc_addr)
            else:
                raise ValueError('Unknown channel type')
            self._poller.register(socket, zmq.POLLIN)
            self._in_sockets[socket] = channel.channel_id

    # TODO set timeout
    def read_message(self) -> ChannelMessage:
        # round-robin read
        data = None
        while self.running and data is None:
            channel_id = self._in_channels[self.cur_read_id].channel_id
            buffer_queue = self._buffer_queues[channel_id]
            if len(buffer_queue) == 0:
                self.cur_read_id = (self.cur_read_id + 1) % len(self._in_channels)
                continue
            else:
                data = buffer_queue.pop()
                break
        self.cur_read_id = (self.cur_read_id + 1) % len(self._in_channels)
        msg = simplejson.loads(data.decode('utf-8'))
        return msg

    async def _reader_loop(self):
        while self.running:
            sockets_and_flags = await self._poller.poll()
            for (socket, _) in sockets_and_flags:
                channel_id = self._in_sockets[socket]

                # TODO check buffer pool capacity
                asyncio.create_task(self._read(socket, channel_id))

    async def _read(self, source: zmq_async.Socket, channel_id: str):
        data = await source.recv()
        self._buffer_queues[channel_id].append(data)

    async def _close_sockets(self):
        await asyncio.gather(*[socket.close(linger=0) for socket in list(self._in_sockets.keys())])
        await self._zmq_ctx.destroy(linger=0)

    def _start_reader_event_loop(self):
        asyncio.set_event_loop(self._reader_event_loop)
        self._init_sockets()
        self._reader_event_loop.run_forever()

    def start(self):
        self.running = True
        self._reader_thread.start()
        asyncio.run_coroutine_threadsafe(self._reader_loop(), self._reader_event_loop)

    def close(self):
        self.running = False
        asyncio.run_coroutine_threadsafe(self._close_sockets(), self._reader_event_loop)
        self._reader_event_loop.call_soon_threadsafe(self._reader_event_loop.stop)
        self._reader_thread.join(timeout=5)
