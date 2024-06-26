import time
from collections import deque

import simplejson
from typing import List, Dict

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

from volga.streaming.runtime.network.buffer.buffer import get_buffer_id, BufferCreator, AckMessageBatch
from volga.streaming.runtime.network.buffer.buffer_pool import BufferPool
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network.utils import send_no_block, rcv_no_block

IN_FLIGHT_LIMIT_PER_CHANNEL = 1000 # max number of un-acked buffers
INN_FLIGHT_TIMEOUT_S = 0.5 # how long to wait before re-sending un-acked buffers


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

        self._in_flight = {c.channel_id: {} for c in self._channels}

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

        # re-sent timed-out in-flight buffers first
        t = time.time()
        for in_flight_buff_id in self._in_flight[channel_id]:
            ts, buffer = self._in_flight[channel_id][in_flight_buff_id]
            if t - ts > INN_FLIGHT_TIMEOUT_S:
                self._in_flight[channel_id][in_flight_buff_id] = (t, buffer)
                sent = send_no_block(socket, buffer)
                if sent:
                    print(f'Re-sent {in_flight_buff_id}')
                else:
                    print(f'Re-sent failed')
                # TODO make RESEND event in stats ?
                return

        # stop sending new buffers if in-flight limit is reached
        if len(self._in_flight[channel_id]) > IN_FLIGHT_LIMIT_PER_CHANNEL:
            # TODO indicate backpressure due to in_flight limit?
            return

        buffer_queue = self._buffer_queues[channel_id]
        if len(buffer_queue) == 0:
            return

        # TODO we should pop buffer only on successful delivery
        buffer = buffer_queue.popleft()
        buffer_id = get_buffer_id(buffer)
        if buffer_id in self._in_flight[channel_id]:
            raise RuntimeError('duplicate buffer_id scheduled')

        self._in_flight[channel_id][buffer_id] = (time.time(), buffer)
        sent = send_no_block(socket, buffer)
        if sent:
            self.stats.inc(StatsEvent.MSG_SENT, channel_id)
            print(f'Sent {buffer_id}, lat: {time.time() - t}')
        else:
            # TODO add delay on retries
            pass
        # TODO should we make a separate container for failed sends or indicate it some other way?

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]

        msg_raw_bytes = rcv_no_block(socket)
        if msg_raw_bytes is None:
            # TODO indicate somehow? Possible backpressure?
            # TODO add delay on retry
            return
        ack_msg_batch = AckMessageBatch.de(msg_raw_bytes)
        for ack_msg in ack_msg_batch.acks:
            # print(f'rcved ack {ack_msg.buffer_id}, lat: {time.time() - t}')
            if channel_id in self._in_flight and ack_msg.buffer_id in self._in_flight[channel_id]:
                self.stats.inc(StatsEvent.ACK_RCVD, channel_id)
                # TODO update buff/msg delivered metric
                # perform ack
                _, buffer = self._in_flight[channel_id][ack_msg.buffer_id]
                del self._in_flight[channel_id][ack_msg.buffer_id]
                # release buffer
                self._buffer_pool.release(len(buffer))

                # TODO pop input queue only on successful delivery. In this case acks should be performed in order
