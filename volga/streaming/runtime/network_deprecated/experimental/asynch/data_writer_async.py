import asyncio
import time
from collections import deque
from threading import Thread

import simplejson
from typing import List, Dict

import zmq

from volga.streaming.api.message.message import Record

import zmq.asyncio as zmq_async

from volga.streaming.runtime.network_deprecated.buffer.buffer_queues import get_buffer_id, BufferQueues
from volga.streaming.runtime.network_deprecated.buffer.buffer import Buffer, AckMessage
from volga.streaming.runtime.network_deprecated.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network_deprecated.experimental.asynch.async_data_handler_base import AsyncDataHandlerBase
from volga.streaming.runtime.network_deprecated.experimental.channel import BiChannel, RemoteBiChannel, LocalBiChannel

# max number of futures per channel, makes sure we do not exhaust io loop
MAX_IN_FLIGHT_FUTURES_PER_CHANNEL = 100000

# max number of not acked buffers, makes sure we do not schedule more if acks are not happening
MAX_IN_FLIGHT_NACKED_PER_CHANNEL = 100000

# how long we wait for a message to be sent before assuming it is inactive (so we stop scheduling more on this channel)
SCHEDULED_BLOCK_TIMEOUT_S = 1


class DataWriterV2Async(AsyncDataHandlerBase):

    def __init__(
        self,
        name: str,
        source_stream_name: str,
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

        self._source_stream_name = source_stream_name

        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in self._channels}
        self._buffer_creator = BufferQueues(self._channels, self._buffer_queues)
        self._buffer_pool = BufferMemoryTracker.instance(node_id=node_id)

        self._flusher_thread = Thread(target=self._flusher_loop)

        self._in_flight_scheduled = {c.channel_id: {} for c in self._channels}
        self._in_flight_nacked = {c.channel_id: {} for c in self._channels}

    def _init_send_sockets(self):
        for channel in self._channels:
            if channel.channel_id in self._send_sockets:
                raise RuntimeError('duplicate channel ids for send')

            # TODO set HWM
            send_socket = self._zmq_ctx.socket(zmq.PUSH)
            send_socket.setsockopt(zmq.LINGER, 0)
            if isinstance(channel, LocalBiChannel):
                send_socket.bind(channel.ipc_addr_out)
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
            if isinstance(channel, LocalBiChannel):
                rcv_socket.connect(channel.ipc_addr_in)
            elif isinstance(channel, RemoteBiChannel):
                raise ValueError('RemoteChannel not supported yet')
            else:
                raise ValueError('Unknown channel type')
            self._rcv_sockets[rcv_socket] = channel.channel_id
            self._rcv_poller.register(rcv_socket, zmq.POLLIN)

    def write_record(self, channel_id: str, record: Record):
        # add sender operator_id
        record.set_stream_name(self._source_stream_name)
        message = record.to_channel_message()
        self._write_message(channel_id, message)

    def _write_message(self, channel_id: str, message: 'ChannelMessage'):
        if not self.running:
            raise RuntimeError('Can not write a message while writer is not running!')
        # TODO serialization perf
        json_str = simplejson.dumps(message)
        buffers = self._buffer_creator._msg_to_buffers(json_str, channel_id=channel_id)
        length_bytes = sum(list(map(lambda b: len(b), buffers)))
        buffer_queue = self._buffer_queues[channel_id]

        if self._buffer_pool.try_acquire(length_bytes):
            for buffer in buffers:
                # TODO we can merge pending buffers in case they are not full
                buffer_queue.append(buffer)
                # print(f'Buff len {len(buffer_queue)}')
        else:
            # TODO indicate buffer pool backpressure
            print('buffer backpressure')

    def _flusher_loop(self):
        print('flusher started')
        # we continuously flush all queues based on configured behaviour
        # (fixed interval, on each new message, on each new buffer)

        # TODO implement fixed interval scheduling
        while self.running:
            for channel_id in self._buffer_queues:
                buffer_queue = self._buffer_queues[channel_id]
                in_flight_scheduled = self._in_flight_scheduled[channel_id]
                in_flight_nacked = self._in_flight_nacked[channel_id]
                if channel_id not in self._send_sockets:
                    continue # not inited yet

                while len(buffer_queue) != 0:
                    # check if we have appropriate in_flight data size, send if ok
                    if len(in_flight_scheduled) > MAX_IN_FLIGHT_FUTURES_PER_CHANNEL:
                        print(f'MAX_IN_FLIGHT_FUTURES_PER_CHANNEL {len(in_flight_scheduled)}')
                        break
                    if len(in_flight_nacked) > MAX_IN_FLIGHT_NACKED_PER_CHANNEL:
                        print(f'MAX_IN_FLIGHT_NACKED_PER_CHANNEL {len(in_flight_nacked)}')
                        break

                    # check if previously scheduled (very first) send is timing out
                    if len(in_flight_scheduled) > 0 and \
                            list(in_flight_scheduled.values())[0][1] - time.time() > SCHEDULED_BLOCK_TIMEOUT_S:
                        print('in_flight timeout')
                        break

                    buffer = buffer_queue.pop()
                    bid = get_buffer_id(buffer)
                    if bid in in_flight_scheduled:
                        raise RuntimeError('duplicate bid scheduled')
                    fut = asyncio.run_coroutine_threadsafe(self._send(channel_id, buffer), self._sender_event_loop)
                    in_flight_scheduled[bid] = (fut, time.time())
                    # print('poped and scheded')

        print(f'flusher finished, running: {self.running}')

    async def _send(self, channel_id: str, buffer: Buffer):
        buffer_id = get_buffer_id(buffer)
        socket = self._send_sockets[channel_id]
        # TODO handle exceptions on send
        t = time.time()
        await socket.send(buffer, zmq.NOBLOCK)
        print(f'Sent {buffer_id}, lat: {time.time() - t}')
        # move from scheduled to nacked
        del self._in_flight_scheduled[channel_id][buffer_id]
        self._in_flight_nacked[channel_id][buffer_id] = (time.time(), buffer)

    async def _receive_loop(self):
        while self.running:
            socks_and_masks = await self._rcv_poller.poll()
            # TODO limit number of pending io tasks
            for (rcv_sock, _) in socks_and_masks:
                asyncio.create_task(self._handle_ack(rcv_sock))

    async def _handle_ack(self, rcv_sock: zmq_async.Socket):
        t = time.time()
        msg_raw = await rcv_sock.recv_string(zmq.NOBLOCK)
        ack_msg = AckMessage(**simplejson.loads(msg_raw))
        print(f'rcved ack {ack_msg.buffer_id}, lat: {time.time() - t}')
        if ack_msg.channel_id in self._in_flight_nacked and ack_msg.buffer_id in self._in_flight_nacked[ack_msg.channel_id]:
            # TODO update buff/msg send metric
            # perform ack
            _, buffer = self._in_flight_nacked[ack_msg.channel_id][ack_msg.buffer_id]
            del self._in_flight_nacked[ack_msg.channel_id][ack_msg.buffer_id]
            # release buffer
            self._buffer_pool.release(len(buffer))


    def start(self):
        super().start()
        # TODO wait for event loop to be set
        time.sleep(0.01)
        asyncio.run_coroutine_threadsafe(self._receive_loop(), self._receiver_event_loop)
        self._flusher_thread.start()

    def close(self):
        # TODO cancel all in-flight tasks
        super().close()
        self._flusher_thread.join(timeout=5)
