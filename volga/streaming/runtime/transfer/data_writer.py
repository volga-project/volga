import time
from collections import deque

import simplejson
from typing import List, Dict, Optional

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

from volga.streaming.runtime.transfer.buffer import get_buffer_id, BufferCreator, AckMessageBatch
from volga.streaming.runtime.transfer.buffer_pool import BufferPool
from volga.streaming.runtime.transfer.config import ZMQConfig, DEFAULT_ZMQ_CONFIG
from volga.streaming.runtime.transfer.data_handler_base import DataHandlerBase

# max number of futures per channel, makes sure we do not exhaust io loop
MAX_IN_FLIGHT_PER_CHANNEL = 100000

# max number of not acked buffers, makes sure we do not schedule more if acks are not happening
MAX_NACKED_PER_CHANNEL = 100000

# how long we wait for a message to be sent before assuming it is inactive (so we stop scheduling more on this channel)
IN_FLIGHT_TIMEOUT_S = 1


class DataWriter(DataHandlerBase):

    class _Stats:
        def __init__(self):
            self.msgs_sent = 0
            self.acks_rcvd = 0

        def __repr__(self):
            return str(self.__dict__)

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq.Context,
        zmq_config: Optional[ZMQConfig] = DEFAULT_ZMQ_CONFIG
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx,
            is_reader=False,
            zmq_config=zmq_config
        )

        self.stats = DataWriter._Stats()
        self._source_stream_name = source_stream_name

        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in self._channels}
        self._buffer_creator = BufferCreator(self._channels, self._buffer_queues)
        self._buffer_pool = BufferPool.instance(node_id=node_id)

        self._nacked = {c.channel_id: {} for c in self._channels}

    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage):
        if not self.running:
            raise RuntimeError('Can not write a message while writer is not running!')
        # TODO serialization perf
        json_str = simplejson.dumps(message)
        buffers = self._buffer_creator.msg_to_buffers(json_str, channel_id=channel_id)
        length_bytes = sum(list(map(lambda b: len(b), buffers)))
        buffer_queue = self._buffer_queues[channel_id]

        if self._buffer_pool.try_acquire(length_bytes):
            for buffer in buffers:
                # TODO we can merge pending buffers in case they are not full
                buffer_queue.append(buffer)
                # print(f'Buff len {len(buffer_queue)}')
        else:
            # TODO indicate buffer pool backpressure
            print('buffer backpressure')

    def _send(self, channel_id: str, socket: zmq.Socket):
        buffer_queue = self._buffer_queues[channel_id]
        if len(buffer_queue) == 0:
            return
        buffer = buffer_queue.pop()
        buffer_id = get_buffer_id(buffer)
        if buffer_id in self._nacked[channel_id]:
            raise RuntimeError('duplicate buffer_id scheduled')
        # TODO handle exceptions on send
        t = time.time()

        # TODO use noblock, handle exceptions
        socket.send(buffer)
        self.stats.msgs_sent += 1
        print(f'Sent {buffer_id}, lat: {time.time() - t}')
        self._nacked[channel_id][buffer_id] = (time.time(), buffer)

    def _rcv(self, channel_id: str, socket: zmq.Socket):
        t = time.time()

        # TODO use NOBLOCK handle exceptions, EAGAIN, etc.
        msg_raw_bytes = socket.recv()
        ack_msg_batch = AckMessageBatch.de(msg_raw_bytes.decode())
        for ack_msg in ack_msg_batch.acks:
            print(f'rcved ack {ack_msg.buffer_id}, lat: {time.time() - t}')
            if channel_id in self._nacked and ack_msg.buffer_id in self._nacked[channel_id]:
                self.stats.acks_rcvd += 1
                # TODO update buff/msg send metric
                # perform ack
                _, buffer = self._nacked[channel_id][ack_msg.buffer_id]
                del self._nacked[channel_id][ack_msg.buffer_id]
                # release buffer
                self._buffer_pool.release(len(buffer))
