import time
from collections import deque

import simplejson
from typing import List, Dict, Optional

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

from volga.streaming.runtime.transfer.buffer import get_buffer_id, BufferCreator, AckMessageBatch
from volga.streaming.runtime.transfer.buffer_pool import BufferPool
from volga.streaming.runtime.transfer.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.transfer.data_handler_base import DataHandlerBase

class DataWriter(DataHandlerBase):

    class _Stats:
        def __init__(self, channels: List[Channel]):
            self.msgs_sent = {c.channel_id: 0 for c in channels}
            self.acks_rcvd = {c.channel_id: 0 for c in channels}

        def inc_msgs_sent(self, channel_id: str, num: Optional[int] = None):
            if num is None:
                self.msgs_sent[channel_id] += 1
            else:
                self.msgs_sent[channel_id] += num

        def inc_acks_rcvd(self, channel_id: str, num: Optional[int] = None):
            if num is None:
                self.acks_rcvd[channel_id] += 1
            else:
                self.acks_rcvd[channel_id] += num

        def __repr__(self):
            return str(self.__dict__)

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq.Context,
        network_config: NetworkConfig = DEFAULT_NETWORK_CONFIG
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx,
            is_reader=False,
            network_config=network_config
        )

        self.stats = DataWriter._Stats(channels)
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
        # block until starts running
        timeout_s = 5
        t = time.time()
        while not self.running and time.time() - t < timeout_s:
            time.sleep(0.01)
        if not self.running:
            raise RuntimeError(f'DataWriter did not start after {timeout_s}s, {message}')

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
        self.stats.inc_msgs_sent(channel_id)
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
                self.stats.inc_acks_rcvd(channel_id)
                # TODO update buff/msg send metric
                # perform ack
                _, buffer = self._nacked[channel_id][ack_msg.buffer_id]
                del self._nacked[channel_id][ack_msg.buffer_id]
                # release buffer
                self._buffer_pool.release(len(buffer))
