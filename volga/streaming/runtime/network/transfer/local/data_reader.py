import logging
import time
from collections import deque
from typing import List, Optional

import zmq

from volga.streaming.runtime.network.buffer.buffer import get_payload, get_buffer_id, AckMessage, AckMessageBatch
from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

import simplejson

from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network.utils import bytes_to_str, rcv_no_block, send_no_block

logger = logging.getLogger("ray")


class DataReader(LocalDataHandler):

    def __init__(
        self,
        name: str,
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
            direction=Direction.RECEIVER,
            network_config=network_config
        )

        self.stats = Stats()

        self._output_queue = deque()

        self._watermarks = {c.channel_id: -1 for c in self._channels}
        self._out_of_order = {c.channel_id: {} for c in self._channels}
        self._acks_queues = {c.channel_id: deque() for c in self._channels}

    def read_message(self) -> List[ChannelMessage]:
        if len(self._output_queue) == 0:
            return []
        buffer = self._output_queue.popleft()
        payload = get_payload(buffer)

        # TODO implement partial messages
        res = []
        for (msg_id, data) in payload:
            msg = simplejson.loads(bytes_to_str(data))
            res.append(msg)
        return res

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
            # for ack_msg in ack_msg_batch.acks:
            #     print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')
        else:
            # TODO add delay on retry
            # put back in queue
            for ack in reversed(acks):
                ack_queue.insert(0, ack)

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.time()

        buffer = rcv_no_block(socket)
        if buffer is None:
            # TODO indicate somehow? Possible backpressure?
            # TODO add delay on retry
            return
        self.stats.inc(StatsEvent.MSG_RCVD, channel_id)
        print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')

        # TODO acquire buffer pool
        buffer_id = get_buffer_id(buffer)
        wm = self._watermarks[channel_id]
        if buffer_id <= wm:
            # drop and resend ack
            ack_msg = AckMessage(buffer_id=buffer_id)
            self._acks_queues[channel_id].append(ack_msg)
            return
        else:
            # TODO we dont want _out_of_order to grow indefinitely, we should limit the size and drop once it is reached
            if buffer_id in self._out_of_order[channel_id]:
                # duplicate, do nothing. Should we ack out_of_order buffers?
                return
            self._out_of_order[channel_id][buffer_id] = buffer

            # check if we have sequential buffers to put in order
            next_wm = wm + 1
            to_del = []
            while next_wm in self._out_of_order[channel_id]:
                self._output_queue.append(self._out_of_order[channel_id][next_wm])

                # ack only when placed onto output queue
                # TODO we should ack only when output queue element is consumed and processed in state
                ack_msg = AckMessage(buffer_id=buffer_id)
                self._acks_queues[channel_id].append(ack_msg)
                to_del.append(next_wm)
                next_wm += 1

            for buff_id in to_del:
                del self._out_of_order[channel_id][buff_id]

            self._watermarks[channel_id] = next_wm - 1
