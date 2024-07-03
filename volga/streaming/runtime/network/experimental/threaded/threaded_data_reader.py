import logging
import time
from collections import deque
from typing import List, Optional

import zmq

from volga.streaming.runtime.network.channel import Channel, ChannelMessage, LocalChannel, RemoteChannel

import simplejson

from volga.streaming.runtime.network.buffer.buffer_queues import get_buffer_id, get_payload
from volga.streaming.runtime.network.buffer.buffer import AckMessage, AckMessageBatch
from volga.streaming.runtime.network.experimental.threaded.threaded_data_handler_base import DataHandlerBase
from volga.streaming.runtime.network.utils import bytes_to_str

logger = logging.getLogger("ray")


class DataReaderV2(DataHandlerBase):
    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq.Context,
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx,
        )

        self._channels = channels
        self._channel_map = { c for c in self._channels}

        self._buffer_queue = deque()
        self._acks_queues = {c.channel_id: deque() for c in self._channels}

    def _init_send_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._send_sockets:
                raise RuntimeError('duplicate channel ids for send')

            # TODO set HWM
            send_socket = self._zmq_ctx.socket(zmq.PUSH)
            send_socket.setsockopt(zmq.LINGER, 0)
            send_socket.setsockopt(zmq.SNDHWM, 10000)
            send_socket.setsockopt(zmq.RCVHWM, 10000)
            send_socket.setsockopt(zmq.SNDBUF, 2000 * 1024)
            send_socket.setsockopt(zmq.RCVBUF, 2000 * 1024)
            if isinstance(channel, LocalChannel):
                send_socket.bind(channel.ipc_addr_in)
            elif isinstance(channel, RemoteChannel):
                raise ValueError('RemoteChannel not supported yet')
            else:
                raise ValueError('Unknown channel type')
            self._send_sockets[channel.channel_id] = send_socket

    def _init_rcv_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._rcv_sockets:
                raise RuntimeError('duplicate channel ids for rcv')

            # TODO set HWM
            rcv_socket = self._zmq_ctx.socket(zmq.PULL)
            rcv_socket.setsockopt(zmq.LINGER, 0)
            rcv_socket.setsockopt(zmq.SNDHWM, 10000)
            rcv_socket.setsockopt(zmq.RCVHWM, 10000)
            rcv_socket.setsockopt(zmq.SNDBUF, 2000 * 1024)
            rcv_socket.setsockopt(zmq.RCVBUF, 2000 * 1024)
            if isinstance(channel, LocalChannel):
                rcv_socket.connect(channel.ipc_addr_out)
            elif isinstance(channel, RemoteChannel):
                raise ValueError('RemoteChannel not supported yet')
            else:
                raise ValueError('Unknown channel type')
            self._rcv_sockets[rcv_socket] = channel.channel_id
            self._rcv_poller.register(rcv_socket, zmq.POLLIN)

    # TODO set timeout
    def read_message(self) -> Optional[ChannelMessage]:
        if len(self._buffer_queue) == 0:
            return None
        buffer = self._buffer_queue.pop()
        payload = get_payload(buffer)

        # TODO implement impartial messages/multiple messages in a buffer
        msg_id, data = payload[0]
        msg = simplejson.loads(bytes_to_str(data))
        return msg

    # def _start_sender_loop(self):
    #     pass

    def _receive_loop(self):
        print('reader rcv loop started')
        while self.running:
            sockets_and_flags = self._rcv_poller.poll()
            for (socket, _) in sockets_and_flags:
                # TODO check buffer pool capacity
                # TODO limit number in-flight reads, indicate channel backpressure if read times-out
                self._read(socket)

    def _read(self, rcv_socket: zmq.Socket):
        t = time.time()
        buffer = rcv_socket.recv(zmq.NOBLOCK)
        print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')
        # TODO check if buffer_id exists to avoid duplicates, re-send ack on duplicate
        # TODO acquire buffer pool
        self._buffer_queue.append(buffer)
        channel_id = self._rcv_sockets[rcv_socket]

        # TODO keep track of a low watermark, schedule ack only if received buff_id is above
        # schedule ack message for sender
        buffer_id = get_buffer_id(buffer)
        ack_msg = AckMessage(buffer_id=buffer_id, channel_id=channel_id)
        self._acks_queues[channel_id].append(ack_msg)

    def _send_loop(self):
        # TODO should we share logic with DataWriter's sender? This way we'll get in_flight limitation + cleaner thread?
        while self.running:
            for channel_id in self._acks_queues:
                # TODO batch acks
                ack_queue = self._acks_queues[channel_id]
                batch_size = 10
                if len(ack_queue) < batch_size:
                    continue
                acks = []
                while len(ack_queue) != 0:
                    ack_msg = ack_queue.schedule_next()
                    acks.append(ack_msg)
                ack_msg_batch = AckMessageBatch(acks=acks)
                self._send_acks(channel_id, ack_msg_batch)

    def _send_acks(self, channel_id: str, ack_msg_batch: AckMessageBatch):
        send_socket = self._send_sockets[channel_id]
        # TODO limit number of in-flights acks?
        bs = ack_msg_batch.ser()
        t = time.time()

        # TODO handle exceptions, EAGAIN, etc., retries
        # send_socket.send_string(data, zmq.NOBLOCK)
        send_socket.send(bs)
        for ack_msg in ack_msg_batch.acks:
            print(f'sent ack {ack_msg.buffer_id}, lat: {time.time() - t}')
