import logging
import time
from collections import deque
from typing import List

import zmq

from volga.streaming.runtime.network_deprecated.buffer.buffer import AckMessage, AckMessageBatch
from volga.streaming.runtime.network_deprecated.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network_deprecated.buffer.serialization.buffer_serializer import JSONBufferSerializer, \
    BufferSerializer
from volga.streaming.runtime.network_deprecated.metrics import Metric
from volga.streaming.runtime.network_deprecated.stats import Stats, StatsEvent
from volga.streaming.runtime.network_deprecated.transfer.io_loop import Direction, IOHandlerType
from volga.streaming.runtime.network_deprecated.channel import Channel, ChannelMessage

from volga.streaming.runtime.network_deprecated.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network_deprecated.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network_deprecated.socket_utils import rcv_no_block, send_no_block

logger = logging.getLogger("ray")


class DataReader(LocalDataHandler):

    def __init__(
        self,
        name: str,
        job_name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq.Context,
        network_config: NetworkConfig = DEFAULT_NETWORK_CONFIG
    ):
        super().__init__(
            job_name=job_name,
            name=name,
            channels=channels,
            zmq_ctx=zmq_ctx,
            direction=Direction.RECEIVER,
            network_config=network_config
        )

        self.stats = Stats()

        self._output_queue = deque()

        self._watermarks = {c.channel_id: -1 for c in self._channels}
        self._out_of_order = {c.channel_id: {} for c in self._channels}
        self._acks_queues = {c.channel_id: deque() for c in self._channels}

        self._buffer_memory_tracker = BufferMemoryTracker.instance(node_id=node_id, job_name=job_name)

    def get_handler_type(self) -> IOHandlerType:
        return IOHandlerType.DATA_READER

    def read_message(self) -> List[ChannelMessage]:
        if len(self._output_queue) == 0:
            return []
        buffer = self._output_queue.popleft()
        channel_id = BufferSerializer.get_channel_id(buffer)
        # release memory
        self._buffer_memory_tracker.release(self.name, _in=False)

        # TODO implement partial messages
        msgs = JSONBufferSerializer.deser(buffer)
        self.metrics_recorder.inc(Metric.NUM_RECORDS_RCVD, self.name, self.get_handler_type(), channel_id, len(msgs))

        return msgs

    def send(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]

        t = time.time()

        ack_queue = self._acks_queues[channel_id]
        ack_batch_size = self._network_config.ack_batch_size
        if len(ack_queue) < ack_batch_size:
            return
        acks = []
        while len(ack_queue) != 0:
            ack_msg = ack_queue.popleft()
            acks.append(ack_msg)
        ack_msg_batch = AckMessageBatch(channel_id=channel_id, acks=acks)
        b = ack_msg_batch.ser()

        sent = send_no_block(socket, b)
        if sent:
            self.stats.inc(StatsEvent.ACK_SENT, channel_id, len(ack_msg_batch.acks))
            self.metrics_recorder.inc(Metric.NUM_BUFFERS_SENT, self.name, self.get_handler_type(), channel_id)
            # for ack_msg in ack_msg_batch.acks:
            #     print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')
        else:
            # TODO add delay on retry
            # put back in queue
            for ack in reversed(acks):
                ack_queue.insert(0, ack)

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.perf_counter()

        has_mem = self._buffer_memory_tracker.try_acquire(self.name, _in=False)
        if has_mem:
            # we will re-acquire memory later, release for now
            self._buffer_memory_tracker.release(self.name, _in=False)

        # We backpressure only if there are messages in out_of_order that can be processed (or empty),
        # otherwise we will have a deadlock
        if (len(self._out_of_order[channel_id]) == 0 or self._watermarks[channel_id] + 1 in self._out_of_order[channel_id]) and not has_mem:
            # TODO indicate memory backpressure?
            return

        buffer = rcv_no_block(socket)
        if buffer is None:
            # TODO indicate somehow? Possible network backpressure?
            # TODO add delay on retry
            return

        self.stats.inc(StatsEvent.MSG_RCVD, channel_id)
        self.metrics_recorder.inc(Metric.NUM_BUFFERS_RCVD, self.name, self.get_handler_type(), channel_id)
        print(f'Rcvd {BufferSerializer.get_buffer_id(buffer)}, lat: {time.perf_counter() - t}')

        buffer_id = BufferSerializer.get_buffer_id(buffer)
        wm = self._watermarks[channel_id]
        if buffer_id <= wm:
            # drop and resend ack
            ack_msg = AckMessage(buffer_id=buffer_id)
            self._acks_queues[channel_id].append(ack_msg)
            return
        else:
            # We don't want out_of_order to grow infinitely and should put a limit on it,
            # however in theory it should not happen - sender will ony send maximum of it's buffer queue size
            # before receiving ack and sending more (which happens only after all _out_of_order is processed)
            if buffer_id in self._out_of_order[channel_id]:
                ack_msg = AckMessage(buffer_id=buffer_id)
                self._acks_queues[channel_id].append(ack_msg)
                # duplicate
                return
            self._out_of_order[channel_id][buffer_id] = buffer

            # check if we have sequential buffers to put in order
            next_wm = wm + 1
            to_del = []
            while next_wm in self._out_of_order[channel_id]:
                succ = self._buffer_memory_tracker.try_acquire(self.name, _in=False)
                if not succ:
                    # not enough mem, backpressure will happen
                    break

                self._output_queue.append(self._out_of_order[channel_id][next_wm])

                # ack only when placed onto output queue
                ack_msg = AckMessage(buffer_id=buffer_id) # TODO we should ack stored buffer id
                self._acks_queues[channel_id].append(ack_msg)
                to_del.append(next_wm)
                next_wm += 1

            for buff_id in to_del:
                del self._out_of_order[channel_id][buff_id]

            self._watermarks[channel_id] = next_wm - 1
