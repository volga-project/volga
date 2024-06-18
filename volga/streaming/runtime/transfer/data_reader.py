import logging
import time
from collections import deque
from typing import List, Optional

import zmq

from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

import simplejson

from volga.streaming.runtime.transfer.buffer import AckMessage, get_buffer_id, get_payload, AckMessageBatch
from volga.streaming.runtime.transfer.config import ZMQConfig, DEFAULT_ZMQ_CONFIG
from volga.streaming.runtime.transfer.data_handler_base import DataHandlerBase
from volga.streaming.runtime.transfer.utils import bytes_to_str

logger = logging.getLogger("ray")


class DataReader(DataHandlerBase):

    class _Stats:
        def __init__(self):
            self.msgs_rcvd = 0
            self.acks_sent = 0

        def __repr__(self):
            return str(self.__dict__)

    def __init__(
        self,
        name: str,
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
            is_reader=True,
            zmq_config=zmq_config
        )

        self.stats = DataReader._Stats()

        self._channels = channels
        self._channel_map = { c for c in self._channels}

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

    def _send(self, channel_id: str, socket: zmq.Socket):
        ack_queue = self._acks_queues[channel_id]
        batch_size = 10
        if len(ack_queue) < batch_size:
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
        self.stats.acks_sent += len(ack_msg_batch.acks)
        for ack_msg in ack_msg_batch.acks:
            print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')

    def _rcv(self, channel_id: str, socket: zmq.Socket):
        t = time.time()

        # TODO NOBLOCK, exceptions, eagain etc.
        buffer = socket.recv()
        self.stats.msgs_rcvd += 1
        print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')
        # TODO check if buffer_id exists to avoid duplicates, re-send ack on duplicate
        # TODO acquire buffer pool
        self._buffer_queue.append(buffer)

        # TODO keep track of a low watermark, schedule ack only if received buff_id is above
        # schedule ack message for sender
        buffer_id = get_buffer_id(buffer)
        ack_msg = AckMessage(buffer_id=buffer_id, channel_id=channel_id)
        self._acks_queues[channel_id].append(ack_msg)
