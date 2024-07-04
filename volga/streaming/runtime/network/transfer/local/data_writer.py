import time
from collections import deque

import simplejson
from typing import List, Dict

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network.buffer.buffer import get_buffer_id, AckMessageBatch, DEFAULT_BUFFER_SIZE
from volga.streaming.runtime.network.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

from volga.streaming.runtime.network.buffer.buffer_queues import BufferQueues
from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network.utils import send_no_block, rcv_no_block

# TODO we want to make sure this limit is low enough to cause backpressure when buffer memory is full
# TODO should this be smaller then default buffer memory capacity per channel
# TODO do we even need this? we should have no more than max buffer queue length of in-flight buffers
IN_FLIGHT_LIMIT_PER_CHANNEL = 1000 # max number of un-acked buffers

INN_FLIGHT_TIMEOUT_S = 0.5 # how long to wait before re-sending un-acked buffers


class DataWriter(LocalDataHandler):

    def __init__(
        self,
        name: str,
        source_stream_name: str,
        job_name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq.Context,
        network_config: NetworkConfig = DEFAULT_NETWORK_CONFIG,
        buffering_policy: BufferingPolicy = BufferPerMessagePolicy(),
        buffering_config: BufferingConfig = BufferingConfig()
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx,
            direction=Direction.SENDER,
            network_config=network_config
        )

        self._job_name = job_name
        self.stats = Stats()
        self._source_stream_name = source_stream_name

        self._buffer_queues = BufferQueues(
            self._channels,
            BufferMemoryTracker.instance(
                node_id=node_id,
                job_name=self._job_name,
                capacity_per_in_channel=buffering_config.capacity_per_in_channel,
                capacity_per_out=buffering_config.capacity_per_out
            ),
            buffering_config.buffer_size,
            buffering_policy
        )

        self._in_flight = {c.channel_id: {} for c in self._channels}

    # TODO we should update logic how to handle backpressure upstream
    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage, block=True, timeout=5) -> bool:
        # block until starts running
        timeout_s = 5
        t = time.time()
        while not self.is_running() and time.time() - t < timeout_s:
            time.sleep(0.01)
        if not self.is_running():
            raise RuntimeError(f'DataWriter did not start after {timeout_s}s')

        backpressure = not self._buffer_queues.append_msg(message, channel_id)
        if not backpressure:
            return True

        if not block:
            return False

        t = time.time()
        while backpressure:
            if time.time() - t > timeout:
                break
            backpressure = not self._buffer_queues.append_msg(message, channel_id)
            time.sleep(0.01)
        # TODO record backpressure stats
        return not backpressure

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

        # schedule next puts a copy of next buffer in queue to in-flight without popping
        # we should pop only when acks are received
        buffer = self._buffer_queues.schedule_next(channel_id)
        if buffer is None:
            return

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
                if ack_msg.buffer_id != get_buffer_id(buffer):
                    raise RuntimeError('buffer_id missmatch')
                del self._in_flight[channel_id][ack_msg.buffer_id]

                # pop input queue on successful delivery
                self._buffer_queues.pop(channel_id=channel_id, buffer_id=ack_msg.buffer_id)
