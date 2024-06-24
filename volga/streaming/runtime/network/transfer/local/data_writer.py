import time
from collections import deque

import simplejson
from typing import List, Dict, Optional

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

from volga.streaming.runtime.network.buffer.buffer import get_buffer_id, BufferCreator, AckMessageBatch
from volga.streaming.runtime.network.buffer.buffer_pool import BufferPool
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler


class DataWriter(LocalDataHandler):

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
            direction=Direction.SENDER,
            network_config=network_config
        )

        self.stats = Stats()
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
        while not self.is_running() and time.time() - t < timeout_s:
            time.sleep(0.01)
        if not self.is_running():
            raise RuntimeError(f'DataWriter did not start after {timeout_s}s')

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

    def send(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
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
        self.stats.inc(StatsEvent.MSG_SENT, channel_id)
        print(f'Sent {buffer_id}, lat: {time.time() - t}')
        self._nacked[channel_id][buffer_id] = (time.time(), buffer)

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.time()

        # TODO handle nacks requests

        # TODO use NOBLOCK handle exceptions, EAGAIN, etc.
        msg_raw_bytes = socket.recv()
        ack_msg_batch = AckMessageBatch.de(msg_raw_bytes)
        for ack_msg in ack_msg_batch.acks:
            # print(f'rcved ack {ack_msg.buffer_id}, lat: {time.time() - t}')
            if channel_id in self._nacked and ack_msg.buffer_id in self._nacked[channel_id]:
                self.stats.inc(StatsEvent.ACK_RCVD, channel_id)
                # TODO update buff/msg delivered metric
                # perform ack
                _, buffer = self._nacked[channel_id][ack_msg.buffer_id]
                del self._nacked[channel_id][ack_msg.buffer_id]
                # release buffer
                self._buffer_pool.release(len(buffer))
