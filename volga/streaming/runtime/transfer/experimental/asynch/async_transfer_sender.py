import asyncio
from threading import Thread
from typing import List, Dict

import zmq.asyncio as zmq_async
import zmq

from volga.streaming.runtime.transfer.experimental.channel import RemoteBiChannel


# router dealer
# https://github.com/zeromq/pyzmq/blob/main/examples/asyncio/helloworld_pubsub_dealerrouter.py


class AsyncTransferSender:

    def __init__(
        self,
        remote_out_channels: List[RemoteBiChannel],
        host_node_id,
    ):
        self._channel_map: Dict[str, RemoteBiChannel] = {c.channel_id: c for c in remote_out_channels}
        self._zmq_ctx = zmq_async.Context.instance(io_threads=64) # TODO configure
        self._host_node_id = host_node_id
        self._poller: zmq_async.Poller = zmq_async.Poller()
        self._local_in_sockets: Dict[zmq_async.Socket, str] = {} # local ipc connections per channel
        self._remote_out_sockets: Dict[zmq_async.Socket, str] = {} # remote tcp connection per target node

        self.running = False
        self._sender_event_loop = asyncio.new_event_loop()
        self._sender_thread = Thread(target=self._start_sender_event_loop)

        for channel in remote_out_channels:
            if channel.channel_id in list(self._local_in_sockets.values()):
                raise RuntimeError('Duplicate channel_id on local socket init')
            if channel.source_node_id != self._host_node_id:
                raise RuntimeError('Node id mismatch')

            # init local ipc
            local_socket = self._zmq_ctx.socket(zmq.PULL)
            local_socket.connect(channel.source_local_ipc_addr_out)
            self._local_in_sockets[local_socket] = channel.channel_id

            # init remote tcp
            if channel.target_node_id in list(self._remote_out_sockets.values()):
                continue
            else:
                remote_socket = self._zmq_ctx.socket(zmq.PUSH)
                remote_socket.bind(f'tcp://{channel.source_node_ip}:{channel.port_out}')
                self._poller.register(remote_socket, zmq.POLLOUT)
                self._remote_out_sockets[remote_socket] = channel.target_node_id

    def _start_sender_event_loop(self):
        asyncio.set_event_loop(self._sender_event_loop)
        self._sender_event_loop.run_forever()

    async def _sender_loop(self):
        while self.running:
            sockets_and_masks = await self._poller.poll()

            # group sockets local_in -> remote_out for each ready remote
            ready_remotes = [socket for (socket, _) in sockets_and_masks if socket in self._remote_out_sockets]
            ready_locals = [socket for (socket, _) in sockets_and_masks if socket in self._local_in_sockets]
            # for each ready remote find any ready local which belongs to the same channel
            for _remote in ready_remotes:
                target_node_id = self._remote_out_sockets[_remote]
                for _local in ready_locals:
                    channel_id = self._local_in_sockets[_local]
                    if self._channel_map[channel_id].target_node_id == target_node_id:
                        # schedule async send
                        asyncio.create_task(self._send(_local, _remote))
                        break

    async def _send(self, source: zmq_async.Socket, target: zmq_async.Socket):
        data = await source.recv()
        await target.send(data)

    def start(self):
        self.running = True
        self._sender_thread.start()
        asyncio.run_coroutine_threadsafe(self._sender_loop(), self._sender_event_loop)

    def stop(self):
        self.running = False
        self._sender_event_loop.call_soon_threadsafe(self._sender_event_loop.stop)
        self._sender_thread.join(timeout=5)