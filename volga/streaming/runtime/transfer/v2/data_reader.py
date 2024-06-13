import asyncio
import logging
import time
from collections import deque
from threading import Thread
from typing import List, Dict, Optional

from volga.streaming.runtime.transfer.channel import Channel, ChannelMessage

import zmq.asyncio as zmq_async
import simplejson

from volga.streaming.runtime.transfer.v2.buffer import Buffer, AckMessage, msg_id, buffer_id
from volga.streaming.runtime.transfer.v2.data_handler_base import DataHandlerBase

logger = logging.getLogger("ray")


class DataReaderV2(DataHandlerBase):
    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq_async.Context,
    ):
        super().__init__(
            name=name,
            channels=channels,
            node_id=node_id,
            zmq_ctx=zmq_ctx
        )

        self._channels = channels
        self._channel_map = {c.channel_id: c for c in self._channels}

        self._buffer_queue = deque()

    # TODO set timeout
    def read_message(self) -> Optional[ChannelMessage]:
        if len(self._buffer_queue) == 0:
            return None
        data = self._buffer_queue.pop()
        msg = simplejson.loads(data.decode('utf-8'))
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
        buffer = await rcv_socket.recv()
        # TODO check if buff_id exists to avoid duplicates, re-send ack on duplicate
        # TODO acquire buffer pool
        self._buffer_queue.append(buffer)
        channel_id = self._rcv_sockets[rcv_socket]
        # TODO batch acks
        asyncio.run_coroutine_threadsafe(self._ack(channel_id, buffer), self._sender_event_loop)

    async def _ack(self, channel_id: str, buffer: Buffer):
        ack_msg = AckMessage(buff_id=buffer_id(buffer), msg_id=msg_id(buffer), channel_id=channel_id)
        send_socket = self._send_sockets[channel_id]

        # TODO limit number of in-flights acks?
        # TODO handle exceptions? retries?
        await send_socket.send_string(ack_msg.ser())

    def start(self):
        super().start()
        # TODO wait for loop to be set
        time.sleep(0.01)
        asyncio.run_coroutine_threadsafe(self._receive_loop(), self._receiver_event_loop)

    def close(self):
        super().close()
