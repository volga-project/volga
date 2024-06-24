import logging
import time
from collections import deque
from typing import List, Optional

import zmq

from volga.streaming.runtime.network.stats import Stats, StatsEvent
from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

import simplejson

from volga.streaming.runtime.network.buffer.buffer import AckMessage, get_buffer_id, get_payload, AckMessageBatch
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network.utils import bytes_to_str

logger = logging.getLogger("ray")

NACK_DELAY_S = 0.2 # how long to wait before nacking out of order messages


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
        self._nacked = {c.channel_id: {} for c in self._channels}
        self._acks_queues = {c.channel_id: deque() for c in self._channels}

    def read_message(self) -> Optional[ChannelMessage]:
        if len(self._output_queue) == 0:
            return None
        buffer = self._output_queue.pop()
        payload = get_payload(buffer)

        # TODO implement impartial messages/multiple messages in a buffer
        msg_id, data = payload[0]
        msg = simplejson.loads(bytes_to_str(data))
        return msg

    def send(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]

        # TODO send nacks first
        nacks = []
        t = time.time()
        for n in self._nacked[channel_id]:
            ts = self._nacked[channel_id][n]
            if t - ts > NACK_DELAY_S:
                nacks.append(n)
        if len(nacks) != 0:
            # TODO compose nack batch and send
            pass

        ack_queue = self._acks_queues[channel_id]
        ack_batch_size = self._network_config.ack_batch_size
        if len(ack_queue) < ack_batch_size:
            return
        acks = []
        while len(ack_queue) != 0:
            ack_msg = ack_queue.pop()
            acks.append(ack_msg)
        ack_msg_batch = AckMessageBatch(channel_id=channel_id, acks=acks)
        b = ack_msg_batch.ser()

        # TODO NOBLOCK, handle exceptions, EAGAIN, etc., retries
        # send_socket.send_string(data, zmq.NOBLOCK)
        socket.send(b)
        self.stats.inc(StatsEvent.ACK_SENT, channel_id, len(ack_msg_batch.acks))
        # for ack_msg in ack_msg_batch.acks:
        #     print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.time()

        # TODO NOBLOCK, exceptions, eagain etc.
        buffer = socket.recv()
        self.stats.inc(StatsEvent.MSG_RCVD, channel_id)
        print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')

        # TODO acquire buffer pool
        buffer_id = get_buffer_id(buffer)
        wm = self._watermarks[channel_id]
        if buffer_id == wm + 1:
            self._output_queue.append(buffer)
            self._watermarks[channel_id] += 1
        elif buffer_id <= wm:
            # TODO
            # drop and resend ack
            return
        else:
            # TODO we dont want _out_of_order to grow indefinitely, we should limit the size and drop once it is reached
            if buffer_id in self._out_of_order[channel_id]:
                raise RuntimeError('Should not happen')
            self._out_of_order[channel_id][buffer_id] = buffer

            # remove from nacked if exists
            if buffer_id in self._nacked[channel_id]:
                del self._nacked[channel_id][buffer_id]

            # check if we have sequential buffers to put in order
            next_wm = wm + 1
            to_del = []
            while next_wm in self._out_of_order[channel_id]:
                self._output_queue.append(self._out_of_order[channel_id][next_wm])
                to_del.append(next_wm)
                next_wm += 1

            for buff_id in to_del:
                del self._out_of_order[channel_id][buff_id]

            self._watermarks[channel_id] = next_wm - 1

            # request nacks for missing buffers
            for nacked_id in range(next_wm, buffer_id):
                if nacked_id not in self._nacked[channel_id]:
                    self._nacked[channel_id][nacked_id] = time.time()

        # TODO we should ack only on succesfull placement in output queue
        ack_msg = AckMessage(buffer_id=buffer_id)
        # self._output_queue.append(buffer)
        self._acks_queues[channel_id].append(ack_msg)
