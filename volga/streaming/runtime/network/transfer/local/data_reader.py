import logging
import time
from collections import deque
from typing import List, Optional

import zmq

from volga.streaming.runtime.network.transfer.io_loop import Direction
from volga.streaming.runtime.network.channel import Channel, ChannelMessage

import simplejson

from volga.streaming.runtime.network.buffer.buffer import AckMessage, get_buffer_id, get_payload, AckMessageBatch
from volga.streaming.runtime.network.config import NetworkConfig, DEFAULT_NETWORK_CONFIG
from volga.streaming.runtime.network.transfer.local.local_data_handler import LocalDataHandler
from volga.streaming.runtime.network.utils import bytes_to_str

logger = logging.getLogger("ray")


class DataReader(LocalDataHandler):

    class _Stats:
        def __init__(self, channels: List[Channel]):
            self.msgs_rcvd = {c.channel_id: 0 for c in channels}
            self.acks_sent = {c.channel_id: 0 for c in channels}

        def inc_msgs_rcvd(self, channel_id: str, num: Optional[int] = None):
            if num is None:
                self.msgs_rcvd[channel_id] += 1
            else:
                self.msgs_rcvd[channel_id] += num

        def inc_acks_sent(self, channel_id: str, num: Optional[int] = None):
            if num is None:
                self.acks_sent[channel_id] += 1
            else:
                self.acks_sent[channel_id] += num

        def __repr__(self):
            return str(self.__dict__)

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

        self.stats = DataReader._Stats(channels)

        self._buffer_queue = deque()
        self._acks_queues = {c.channel_id: deque() for c in self._channels}

    def read_message(self) -> Optional[ChannelMessage]:
        if len(self._buffer_queue) == 0:
            return None
        buffer = self._buffer_queue.pop()
        payload = get_payload(buffer)

        # TODO implement impartial messages/multiple messages in a buffer
        msg_id, data = payload[0]
        msg = simplejson.loads(bytes_to_str(data))
        return msg

    def send(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        ack_queue = self._acks_queues[channel_id]
        ack_batch_size = self._network_config.ack_batch_size
        if len(ack_queue) < ack_batch_size:
            return
        acks = []
        while len(ack_queue) != 0:
            ack_msg = ack_queue.pop()
            acks.append(ack_msg)
        ack_msg_batch = AckMessageBatch(acks=acks)
        data = ack_msg_batch.ser().encode()
        t = time.time()

        # TODO handle exceptions, EAGAIN, etc., retries
        # send_socket.send_string(data, zmq.NOBLOCK)
        socket.send(data)
        self.stats.inc_acks_sent(channel_id, len(ack_msg_batch.acks))
        # for ack_msg in ack_msg_batch.acks:
        #     print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')

    def rcv(self, socket: zmq.Socket):
        channel_id = self._socket_to_ch[socket]
        t = time.time()

        # TODO NOBLOCK, exceptions, eagain etc.
        buffer = socket.recv()
        self.stats.inc_msgs_rcvd(channel_id)
        # print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')
        # TODO check if buffer_id exists to avoid duplicates, re-send ack on duplicate
        # TODO acquire buffer pool
        self._buffer_queue.append(buffer)

        # TODO keep track of a low watermark, schedule ack only if received buff_id is above
        # schedule ack message for sender
        buffer_id = get_buffer_id(buffer)
        ack_msg = AckMessage(buffer_id=buffer_id, channel_id=channel_id)
        self._acks_queues[channel_id].append(ack_msg)
