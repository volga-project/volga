from queue import Queue
from threading import Thread

import simplejson
from typing import List, Dict

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

import zmq

from volga.streaming.runtime.transfer.v2.buffer_pool import BufferPool, msg_to_buffers


class DataWriter:

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        output_channels: List[Channel],
    ):
        self.name = name
        self._buffer_pools: Dict[str, BufferPool] = {}
        self._buffer_queues: Dict[str, Queue] = {}
        self.running = False
        self._flusher_thread = Thread(target=self._flusher_loop)

        self.source_stream_name = source_stream_name
        self.out_channels = output_channels

        self.sockets_and_contexts = {}
        for channel in self.out_channels:
            if channel.channel_id in self.sockets_and_contexts:
                raise RuntimeError('duplicate channel ids')
            context = zmq.Context()

            # TODO set HWM
            socket = context.socket(zmq.PUSH)
            socket.setsockopt(zmq.LINGER, 0)
            socket.bind(f'tcp://127.0.0.1:{channel.source_port}')
            self.sockets_and_contexts[channel.channel_id] = (socket, context)

    def start(self):
        self.running = True
        self._flusher_thread.start()

    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self.source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage):
        # TODO serialization perf
        json_str = simplejson.dumps(message)
        buffers = msg_to_buffers(json_str) # this should augment with length, msg_id header
        length_bytes = sum(list(map(lambda b: len(b), buffers)))
        buffer_pool = self._buffer_pools[channel_id]
        buffer_queue = self._buffer_queues[channel_id]
        if buffer_pool.can_acquire(length_bytes):
            buffer_pool.acquire(length_bytes)
            for buffer in buffers:
                # TODO we can merge pending buffers in case they are not full
                buffer_queue.put(buffer)
        else:
            # TODO indicate buffer pool backpressure
            pass

    def _flusher_loop(self):
        # we continuously flush all messages without delay
        # downstream receiver (either local data reader or transfer actor) will handle all the scheduling
        while self.running:
            for channel_id in self._buffer_queues:
                _buffer_queue = self._buffer_queues[channel_id]
                _buffer_pool = self._buffer_pools[channel_id]
                socket = self.sockets_and_contexts[channel_id][0]
                while not _buffer_queue.empty():
                    buffer = _buffer_queue.get()
                    socket.send(buffer) # TODO indicate socket/channel backpressure if this blocks
                    _buffer_pool.release(len(buffer))

    def close(self):
        self.running = False
        self._flusher_thread.join(timeout=5)
        # TODO wait for buffer queues to get empty?
        # cleanup sockets and contexts for all channels
        for channel_id in self.sockets_and_contexts:
            socket = self.sockets_and_contexts[channel_id][0]
            context = self.sockets_and_contexts[channel_id][1]
            socket.close(linger=0)
            context.destroy(linger=0)
