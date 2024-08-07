import time
from typing import List

import zmq

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network_deprecated.buffer.buffer import AckMessageBatch
from volga.streaming.runtime.network_deprecated.buffer.buffering_config import BufferingConfig
from volga.streaming.runtime.network_deprecated.buffer.buffering_policy import BufferingPolicy, PeriodicPartialFlushPolicy, \
    BufferPerMessagePolicy
from volga.streaming.runtime.network_deprecated.buffer.serialization.buffer_serializer import BufferSerializer
from volga.streaming.runtime.network_deprecated.metrics import Metric
from volga.streaming.runtime.network_deprecated.stats import Stats, StatsEvent
from volga.streaming.runtime.network_deprecated.transfer.io_loop import Direction, IOHandlerType
from volga.streaming.runtime.network_deprecated.channel import Channel, ChannelMessage

from volga.streaming.runtime.network_deprecated.buffer.buffer_queues import BufferQueues
from volga.streaming.runtime.network_deprecated.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network_deprecated.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network_deprecated.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network_deprecated.socket_utils import rcv_no_block, send_no_block

# TODO we want to make sure this limit is low enough to cause backpressure when buffer memory is full
# TODO should this be smaller then default buffer memory capacity per channel
# TODO do we even need this? we should have no more than max buffer queue length of in-flight buffers
IN_FLIGHT_LIMIT_PER_CHANNEL = 1000 # max number of un-acked buffers

IN_FLIGHT_TIMEOUT_S = 5 # how long to wait before re-sending un-acked buffers


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
            job_name=job_name,
            name=name,
            channels=channels,
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

    def get_handler_type(self) -> IOHandlerType:
        return IOHandlerType.DATA_WRITER

    # TODO we should update logic how to handle backpressure upstream
    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: ChannelMessage, block=True, timeout=5) -> bool:
        # block until starts running
        timeout_s = 10
        t = time.time()
        while not self.is_running() and time.time() - t < timeout_s:
            time.sleep(0.01)
        if not self.is_running():
            raise RuntimeError(f'DataWriter did not start after {timeout_s}s')

        backpressure = not self._buffer_queues.append_msg(message, channel_id)
        if not backpressure:
            self.metrics_recorder.inc(Metric.NUM_RECORDS_SENT, self.name, self.get_handler_type(), channel_id)
            return True

        if not block:
            return False

        t = time.time()
        while backpressure:
            if time.time() - t > timeout:
                break
            backpressure = not self._buffer_queues.append_msg(message, channel_id)
            time.sleep(0.01)
        # TODO record backpressure metrics

        if not backpressure:
            self.metrics_recorder.inc(Metric.NUM_RECORDS_SENT, self.name, self.get_handler_type(), channel_id)
        return not backpressure

    def send(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.perf_counter()
        # re-sent timed-out in-flight buffers first
        for in_flight_buff_id in self._in_flight[channel_id]:
            ts, buffer = self._in_flight[channel_id][in_flight_buff_id]
            if t - ts > IN_FLIGHT_TIMEOUT_S:
                self._in_flight[channel_id][in_flight_buff_id] = (t, buffer)
                sent = send_no_block(socket, buffer)
                if sent:
                    self.metrics_recorder.inc(Metric.NUM_BUFFERS_RESENT, self.name, self.get_handler_type(), channel_id)
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

        buffer_id = BufferSerializer.get_buffer_id(buffer)
        if buffer_id in self._in_flight[channel_id]:
            raise RuntimeError('duplicate buffer_id scheduled')

        self._in_flight[channel_id][buffer_id] = (time.perf_counter(), buffer)
        _ts = time.perf_counter()
        sent = send_no_block(socket, buffer)
        if sent:
            print(f'Sent {buffer_id}, lat: {time.perf_counter() - _ts}')
            self.stats.inc(StatsEvent.MSG_SENT, channel_id)
            self.metrics_recorder.inc(Metric.NUM_BUFFERS_SENT, self.name, self.get_handler_type(), channel_id)
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

        self.metrics_recorder.inc(Metric.NUM_BUFFERS_RCVD, self.name, self.get_handler_type(), channel_id)
        ack_msg_batch = AckMessageBatch.de(msg_raw_bytes)
        for ack_msg in ack_msg_batch.acks:
            if channel_id in self._in_flight and ack_msg.buffer_id in self._in_flight[channel_id]:
                self.stats.inc(StatsEvent.ACK_RCVD, channel_id)
                self.metrics_recorder.inc(Metric.NUM_BUFFERS_DELIVERED, self.name, self.get_handler_type(), channel_id)

                # TODO num records delivered

                ts, buffer = self._in_flight[channel_id][ack_msg.buffer_id]
                if ack_msg.buffer_id != BufferSerializer.get_buffer_id(buffer):
                    raise RuntimeError('buffer_id missmatch')

                latency = (time.perf_counter() - ts) * 1000
                # TODO report latency
                # self.metrics_recorder.latency(latency, self.name, channel_id)
                # print(latency)

                # perform ack
                del self._in_flight[channel_id][ack_msg.buffer_id]

                # pop input queue on successful delivery
                self._buffer_queues.pop(channel_id=channel_id, buffer_id=ack_msg.buffer_id)
