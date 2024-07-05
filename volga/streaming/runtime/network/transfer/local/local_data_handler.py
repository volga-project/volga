import os
from abc import ABC
from typing import List, Dict, Tuple

import zmq

from volga.streaming.runtime.network.channel import Channel, LocalChannel, RemoteChannel, ipc_path_from_addr
import zmq.asyncio as zmq_async

from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network.config import NetworkConfig
from volga.streaming.runtime.network.transfer.io_loop import IOHandler, Direction
from volga.streaming.runtime.network.socket_utils import configure_socket, SocketMetadata, SocketOwner, SocketRole


# Base class for local DataReader and DataWriter
class LocalDataHandler(IOHandler, ABC):

    def __init__(
        self,
        name: str,
        channels: List[Channel],
        node_id: str,
        zmq_ctx: zmq_async.Context,
        direction: Direction,
        network_config: NetworkConfig
    ):
        super().__init__(
            channels=channels,
            zmq_ctx=zmq_ctx,
            direction=direction,
            network_config=network_config
        )

        self._is_reader = direction == Direction.RECEIVER
        self.name = name

        self._ch_to_socket: Dict[str, zmq.Socket] = {}
        self._socket_to_ch: Dict[zmq.Socket, str] = {}

    def init_sockets(self) -> List[Tuple[SocketMetadata, zmq.Socket]]:
        sockets = []
        for channel in self._channels:
            if channel.channel_id in self._ch_to_socket:
                raise RuntimeError('duplicate channel ids')

            socket = self._zmq_ctx.socket(zmq.PAIR)
            socket_owner = SocketOwner.CLIENT

            # created ipc path if not exists
            # TODO we should clean it up on socket deletion
            if isinstance(channel, LocalChannel):
                ipc_path = ipc_path_from_addr(channel.ipc_addr)
                os.makedirs(ipc_path, exist_ok=True)
            elif isinstance(channel, RemoteChannel):
                ipc_path = ipc_path_from_addr(
                    channel.target_local_ipc_addr if self._is_reader else channel.source_local_ipc_addr
                )
                os.makedirs(ipc_path, exist_ok=True)

            # configure
            zmq_config = self._network_config.zmq
            if zmq_config is not None:
                configure_socket(socket, zmq_config)

            if isinstance(channel, LocalChannel):
                # connects to another local DataReader/DataWriter instance
                if self._is_reader:
                    socket.connect(channel.ipc_addr)
                    socket_role = SocketRole.CONNECT
                else:
                    socket.bind(channel.ipc_addr)
                    socket_role = SocketRole.BIND
            elif isinstance(channel, RemoteChannel):
                if self._is_reader:
                    # connects to receiver RemoteTransferHandler
                    socket.connect(channel.target_local_ipc_addr)
                    socket_role = SocketRole.CONNECT
                else:
                    # connects to sender RemoteTransferHandler
                    socket.bind(channel.source_local_ipc_addr)
                    socket_role = SocketRole.BIND
            else:
                raise ValueError('Unknown channel type')
            self._ch_to_socket[channel.channel_id] = socket
            self._socket_to_ch[socket] = channel.channel_id
            sockets.append((
                SocketMetadata(owner=socket_owner, role=socket_role, channel_id=channel.channel_id),
                socket
            ))

        return sockets

    def close_sockets(self):
        for c in self._channels:
            self._ch_to_socket[c.channel_id].close(linger=0)
