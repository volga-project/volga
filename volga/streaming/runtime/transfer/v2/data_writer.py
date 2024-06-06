import asyncio
import time
from collections import deque
from threading import Thread

import simplejson
from typing import List, Dict

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage, LocalChannel, RemoteChannel

import zmq
import zmq.asyncio as zmq_async

from volga.streaming.runtime.transfer.v2.buffer import buffer_id
from volga.streaming.runtime.transfer.v2.buffer_pool import BufferPool, Buffer
from volga.streaming.runtime.transfer.v2.utils import msg_to_buffers

# max number of futures per channel, makes sure we do not exhaust io loop
MAX_IN_FLIGHT_FUTURES_PER_CHANNEL = 10

# max number of not acked buffers, makes sure we do not schedule more if acks are not happening
MAX_IN_FLIGHT_NACKED_PER_CHANNEL = 10

# how long we wait for a message to be sent before assuming it is inactive (so we stop scheduling more on this channel)
SCHEDULED_BLOCK_TIMEOUT_S = 1


class DataWriterV2:

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        output_channels: List[Channel],
    ):
        self.name = name
        self.running = False
        self._sender_event_loop = None
        self._sender_thread = Thread(target=self._start_sender_event_loop)
        self._flusher_thread = Thread(target=self._flusher_loop)

        self.source_stream_name = source_stream_name
        self._out_channels = output_channels

        self._buffer_pools: Dict[str, BufferPool] = {c.channel_id: BufferPool() for c in self._out_channels}
        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in self._out_channels}

        self._zmq_ctx = None
        self._out_sockets = {}
        self._in_flight_scheduled = {c.channel_id: {} for c in self._out_channels}
        self._in_flight_nacked = {c.channel_id: {} for c in self._out_channels}
        self._buffer_id_increments = {c.channel_id: 0 for c in self._out_channels}

    def _init_sockets(self):
        self._zmq_ctx = zmq_async.Context.instance(io_threads=64) # TODO configure
        for channel in self._out_channels:
            if channel.channel_id in self._out_sockets:
                raise RuntimeError('duplicate channel ids')

            # TODO set HWM
            socket = self._zmq_ctx.socket(zmq.PUSH)
            socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalChannel):
                socket.bind(channel.ipc_addr)
            elif isinstance(channel, RemoteChannel):
                socket.bind(channel.source_local_ipc_addr)
            else:
                raise ValueError('Unknown channel type')
            self._out_sockets[channel.channel_id] = socket

    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self.source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage):
        # TODO serialization perf
        json_str = simplejson.dumps(message)
        buffers = msg_to_buffers(json_str) # this should augment data with BufferHeader
        length_bytes = sum(list(map(lambda b: len(b), buffers)))
        buffer_pool = self._buffer_pools[channel_id]
        buffer_queue = self._buffer_queues[channel_id]
        if buffer_pool.can_acquire(length_bytes):
            buffer_pool.acquire(length_bytes)
            for buffer in buffers:
                # TODO we can merge pending buffers in case they are not full
                buffer_queue.append(buffer)
        else:
            # TODO indicate buffer pool backpressure
            pass

    def _flusher_loop(self):
        # we continuously flush all queues based on configured behavior
        # (fixed interval, on each message, on each buffer)
        while self.running:
            for channel_id in self._buffer_queues:
                buffer_queue = self._buffer_queues[channel_id]
                in_flight_scheduled = self._in_flight_scheduled[channel_id]
                in_flight_nacked = self._in_flight_scheduled[channel_id]
                if channel_id not in self._out_sockets:
                    continue # not inited yet
                while len(buffer_queue) != 0:
                    # check if we have appropriate in_flight data size, send if ok


                    buffer = buffer_queue.pop()
                    bid = buffer_id(buffer)
                    if bid in in_flight_scheduled:
                        raise RuntimeError('duplicate bid scheduled')
                    fut = asyncio.run_coroutine_threadsafe(self._send(channel_id, buffer), self._sender_event_loop)
                    in_flight_scheduled[bid] = (fut, time.time())


    async def _send(self, channel_id: str, buffer: Buffer):
        socket = self._out_sockets[channel_id]
        await socket.send(buffer)
        # remove from in-flight scheduled

    def _start_sender_event_loop(self):
        self._sender_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._sender_event_loop)
        self._init_sockets()
        self._sender_event_loop.run_forever()

    def start(self):
        self.running = True
        self._sender_thread.start()
        self._flusher_thread.start()

    async def _close_sockets(self):
        # TODO cancel all in-flight
        await asyncio.gather(*[self._out_sockets[channel_id].close(linger=0) for channel_id in self._out_channels])
        await self._zmq_ctx.destroy(linger=0)

    def close(self):
        self.running = False
        self._flusher_thread.join(timeout=5)
        asyncio.run_coroutine_threadsafe(self._close_sockets(), self._sender_event_loop)
        # TODO call loop stop only after _close_sockets was awaited
        self._sender_event_loop.call_soon_threadsafe(self._sender_event_loop.stop)
        self._sender_thread.join(timeout=5)
