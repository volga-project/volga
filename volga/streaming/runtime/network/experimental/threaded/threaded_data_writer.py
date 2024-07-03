import time
from collections import deque
from threading import Thread

import simplejson
from typing import List, Dict

import zmq
from zmq import MessageTracker

from volga.streaming.api.message.message import Record
from volga.streaming.runtime.network.channel import Channel, ChannelMessage, LocalChannel, RemoteChannel


from volga.streaming.runtime.network.buffer.buffer_queues import get_buffer_id, BufferQueues
from volga.streaming.runtime.network.buffer.buffer import Buffer, AckMessageBatch
from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network.experimental.threaded.threaded_data_handler_base import DataHandlerBase

# max number of futures per channel, makes sure we do not exhaust io loop
MAX_IN_FLIGHT_PER_CHANNEL = 100000

# max number of not acked buffers, makes sure we do not schedule more if acks are not happening
MAX_NACKED_PER_CHANNEL = 100000

# how long we wait for a message to be sent before assuming it is inactive (so we stop scheduling more on this channel)
IN_FLIGHT_TIMEOUT_S = 1


class DataWriterV2(DataHandlerBase):

    def __init__(
        self,
        name: str,
        source_stream_name: str,
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

        self._source_stream_name = source_stream_name

        self._buffer_queues: Dict[str, deque] = {c.channel_id: deque() for c in self._channels}
        self._buffer_creator = BufferQueues(self._channels, self._buffer_queues)
        self._buffer_pool = BufferMemoryTracker.instance(node_id=node_id)

        self._in_flight = {c.channel_id: {} for c in self._channels}
        self._nacked = {c.channel_id: {} for c in self._channels}

        self._cleaner_thread = Thread(target=self._cleaner_loop)

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
                send_socket.bind(channel.ipc_addr_out)
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
                rcv_socket.connect(channel.ipc_addr_in)
            elif isinstance(channel, RemoteChannel):
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

    def _write_message(self, channel_id: str, message: ChannelMessage):
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

    def _send_loop(self):
        print('sender started')
        # we continuously flush all queues based on configured behaviour
        # (fixed interval, on each new message, on each new buffer)

        # TODO implement fixed interval scheduling
        while self.running:
            for channel_id in self._buffer_queues:
                buffer_queue = self._buffer_queues[channel_id]
                in_flight = self._in_flight[channel_id]
                nacked = self._nacked[channel_id]
                if channel_id not in self._send_sockets:
                    continue # not inited yet

                while len(buffer_queue) != 0:
                    # check if we have appropriate in_flight data size, send if ok
                    if len(in_flight) > MAX_IN_FLIGHT_PER_CHANNEL:
                        print(f'MAX_IN_FLIGHT_PER_CHANNEL {len(in_flight)}')
                        break
                    if len(nacked) > MAX_NACKED_PER_CHANNEL:
                        print(f'MAX_NACKED_PER_CHANNEL {len(nacked)}')
                        break

                    # check if previously scheduled (very first) send is timing out
                    if len(in_flight) > 0 and \
                            list(in_flight.values())[0][1] - time.time() > IN_FLIGHT_TIMEOUT_S:
                        print('in_flight timeout')
                        break

                    buffer = buffer_queue.pop()
                    self._send(channel_id, buffer)
                    # print('poped and scheded')

        print(f'sender finished, running: {self.running}')

    def _send(self, channel_id: str, buffer: Buffer):
        buffer_id = get_buffer_id(buffer)
        if buffer_id in self._nacked[channel_id]:
            raise RuntimeError('duplicate buffer_id scheduled')
        socket = self._send_sockets[channel_id]
        # TODO handle exceptions on send
        t = time.time()
        # TODO handle exceptions, EAGAIN, etc.
        # tracker = socket.send(buffer, copy=False, track=True)
        tracker = []
        socket.send(buffer)

        print(f'Sent {buffer_id}, lat: {time.time() - t}')
        self._in_flight[channel_id][buffer_id] = (tracker, time.time())
        self._nacked[channel_id][buffer_id] = (time.time(), buffer)

    # def _start_receiver_loop(self):
    #     pass

    def _receive_loop(self):
        while self.running:
            socks_and_masks = self._rcv_poller.poll()
            # TODO limit number of pending reads tasks
            for (rcv_sock, _) in socks_and_masks:
                self._handle_ack(rcv_sock)

    def _handle_ack(self, rcv_sock: zmq.Socket):
        t = time.time()

        # TODO handle exceptions, EAGAIN, etc.
        # msg_raw = rcv_sock.recv_string(zmq.NOBLOCK)
        msg_raw_bytes = rcv_sock.recv()
        ack_msg_batch = AckMessageBatch.de(msg_raw_bytes)
        for ack_msg in ack_msg_batch.acks:
            print(f'rcved ack {ack_msg.buffer_id}, lat: {time.time() - t}')
            if ack_msg.channel_id in self._nacked and ack_msg.buffer_id in self._nacked[ack_msg.channel_id]:
                # TODO update buff/msg send metric
                # perform ack
                _, buffer = self._nacked[ack_msg.channel_id][ack_msg.buffer_id]
                del self._nacked[ack_msg.channel_id][ack_msg.buffer_id]
                # release buffer
                self._buffer_pool.release(len(buffer))

    def _cleaner_loop(self):
        while self.running:
            for channel_id in self._in_flight:
                to_del = []
                for buffer_id in self._in_flight[channel_id].keys():
                    tracker, scheduled_at = self._in_flight[channel_id][buffer_id]
                    assert isinstance(tracker, MessageTracker)
                    if tracker.done:
                        to_del.append(buffer_id)
                    else:
                        pass
                        # TODO track timeout, backpressure writes ? Or is it already done in sender loop?
                for buffer_id in to_del:
                    del self._in_flight[channel_id][buffer_id]

    def start(self):
        super().start()
        # self._cleaner_thread.start()

    def close(self):
        super().close()
        # TODO cancel or wait all in-flight msgs?
        self._cleaner_thread.join(timeout=5)
