import asyncio
import logging
import time
from collections import deque
from typing import List, Optional

import zmq

import zmq.asyncio as zmq_async
import simplejson

from volga.streaming.runtime.network.buffer.buffer_queues import get_buffer_id, get_payload
from volga.streaming.runtime.network.buffer.buffer import Buffer, AckMessage
from volga.streaming.runtime.network.experimental.asynch.async_data_handler_base import AsyncDataHandlerBase
from volga.streaming.runtime.network.experimental.channel import BiChannel, LocalBiChannel, RemoteBiChannel
from volga.streaming.runtime.network.byte_utils import bytes_to_str

logger = logging.getLogger("ray")


class DataReaderV2Async(AsyncDataHandlerBase):
    def __init__(
        self,
        name: str,
        channels: List[BiChannel],
        node_id: str,
        zmq_ctx: zmq_async.Context,
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx,
        )

        self._channels = channels
        self._channel_map = {c.channel_id: c for c in self._channels}

        self._buffer_queue = deque()

    def _init_send_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._send_sockets:
                raise RuntimeError('duplicate channel ids for send')

            # TODO set HWM
            send_socket = self._zmq_ctx.socket(zmq.PUSH)
            send_socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalBiChannel):
                send_socket.bind(channel.ipc_addr_in)
            elif isinstance(channel, RemoteBiChannel):
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

    async def _receive_loop(self):
        print('reader rcv loop started')
        while self.running:
            sockets_and_flags = await self._rcv_poller.poll()
            for (socket, _) in sockets_and_flags:
                # TODO check buffer pool capacity
                # TODO limit number in-flight reads, indicate channel backpressure if read times-out
                asyncio.create_task(self._read(socket))

    async def _read(self, rcv_socket: zmq_async.Socket):
        t = time.time()
        buffer = await rcv_socket.recv(zmq.NOBLOCK)
        print(f'Rcvd {get_buffer_id(buffer)}, lat: {time.time() - t}')
        # TODO check if buffer_id exists to avoid duplicates, re-send ack on duplicate
        # TODO acquire buffer pool
        self._buffer_queue.append(buffer)
        channel_id = self._rcv_sockets[rcv_socket]
        # TODO keep track of a low watermark, ack only if received buff_id is above
        # TODO batch acks
        # asyncio.run_coroutine_threadsafe(self._ack(channel_id, buffer), self._sender_event_loop)
        asyncio.create_task(self._send_ack(channel_id, buffer))

    async def _send_ack(self, channel_id: str, buffer: Buffer):
        buffer_id = get_buffer_id(buffer)
        ack_msg = AckMessage(buffer_id=buffer_id)
        send_socket = self._send_sockets[channel_id]

        # TODO limit number of in-flights acks?
        # TODO handle exceptions? retries?
        data = simplejson.dumps(ack_msg.dict())
        # print(data)
        # raise
        t = time.time()
        await send_socket.send_string(data, zmq.NOBLOCK)
        print(f'sent ack {buffer_id}, lat: {time.time() - t}')

    def start(self):
        super().start()
        # TODO wait for even loop to be set
        time.sleep(0.01)
        asyncio.run_coroutine_threadsafe(self._receive_loop(), self._receiver_event_loop)

    def close(self):
        super().close()
