import asyncio
from threading import Thread
from typing import List, Dict
import zmq.asyncio as zmq_async
import zmq

from volga.streaming.runtime.transfer.experimental.channel import RemoteBiChannel


# https://stackoverflow.com/questions/64201043/why-does-pyzmq-subscriber-behave-differently-with-asyncio
class AsyncTransferReceiver:

    def __init__(
        self,
        remote_in_channels: List[RemoteBiChannel],
        host_node_id,
    ):
        self._channel_map: Dict[str, RemoteBiChannel] = {c.channel_id: c for c in remote_in_channels}
        self._zmq_ctx = zmq_async.Context.instance(io_threads=64) # TODO configure
        self._host_node_id = host_node_id
        self._poller: zmq_async.Poller = zmq_async.Poller()
        self._local_out_sockets: Dict[str, zmq_async.Socket] = {} # local ipc connections per channel
        self._remote_in_sockets: Dict[zmq_async.Socket, str] = {} # remote tcp connection per source node

        self.running = False
        self._receiver_event_loop = asyncio.new_event_loop()
        self._receiver_thread = Thread(target=self._start_receiver_event_loop)

        for channel in remote_in_channels:
            if channel.channel_id in self._local_out_sockets:
                raise RuntimeError('Duplicate channel_id on local socket init')
            if channel.target_node_id != self._host_node_id:
                raise RuntimeError('Node id mismatch')

            # init local ipc
            local_socket = self._zmq_ctx.socket(zmq.PUSH)
            local_socket.connect(channel.target_local_ipc_addr_out)
            self._poller.register(local_socket, zmq.POLLOUT)
            self._local_out_sockets[channel.channel_id] = local_socket

            # init remote tcp
            if channel.source_node_id in list(self._remote_in_sockets.values()):
                continue
            else:
                remote_socket = self._zmq_ctx.socket(zmq.PULL)
                remote_socket.bind(f'tcp://{channel.target_node_ip}:{channel.port_out}')
                self._poller.register(remote_socket, zmq.POLLIN)
                self._remote_in_sockets[remote_socket] = channel.source_node_id

    def _start_receiver_event_loop(self):
        asyncio.set_event_loop(self._receiver_event_loop)
        self._receiver_event_loop.run_forever()

    async def _receiver_loop(self):
        while self.running:
            sockets_and_masks = await self._poller.poll()

            transfers = []
            for (_remote, _) in sockets_and_masks:
                # schedule async receive
                asyncio.run_coroutine_threadsafe(self._receive(_remote), self._receiver_event_loop)

    async def _receive(self, source: zmq_async.Socket):
        data = await source.recv()
        channel_id = self._get_channel_id(data)
        target = self._local_out_sockets[channel_id]
        await target.send(data)

    def _get_channel_id(self, data) -> str:
        pass

    def start(self):
        self.running = True
        self._receiver_thread.start()
        asyncio.run_coroutine_threadsafe(self._receiver_loop(), self._receiver_event_loop)

    def stop(self):
        self.running = False
        self._receiver_event_loop.call_soon_threadsafe(self._receiver_event_loop.stop)
        self._receiver_thread.join(timeout=5)
