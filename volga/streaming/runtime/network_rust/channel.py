from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from volga_rust import RustLocalChannel, RustRemoteChannel

ChannelMessage = Dict[str, Any]


class Channel(ABC):
    def __init__(
        self,
        channel_id: str
    ):
        self.channel_id = channel_id

    @abstractmethod
    def to_rust_channel(self) -> Union[RustLocalChannel|RustRemoteChannel]:
        raise NotImplementedError()


# connects two actors on the same node via a zmq.PAIR ipc connection
class LocalChannel(Channel):

    def __init__(
        self,
        channel_id: str,
        ipc_addr: str,
    ):
        super().__init__(channel_id=channel_id)
        self.ipc_addr = ipc_addr

    def to_rust_channel(self) -> RustLocalChannel:
        return RustLocalChannel(self.channel_id, self.ipc_addr)


# connects two actors on different nodes
# 3-part channel: local zmq.PAIR ipc on source, source <-> target  zmq.PAIR TCP, local zmq.PAIR ipc on target
class RemoteChannel(Channel):

    def __init__(
        self,
        channel_id: str,
        source_local_ipc_addr: str,
        source_node_ip: str,
        source_node_id: str,
        target_local_ipc_addr: str,
        target_node_ip: str,
        target_node_id: str,
        port: int,
    ):
        super().__init__(channel_id=channel_id)
        self.source_local_ipc_addr = source_local_ipc_addr
        self.target_local_ipc_addr = target_local_ipc_addr
        self.source_node_ip = source_node_ip
        self.target_node_ip = target_node_ip
        self.source_node_id = source_node_id
        self.target_node_id = target_node_id
        self.port = port

    def to_rust_channel(self) -> RustRemoteChannel:
        return RustRemoteChannel(
            self.source_local_ipc_addr,
            self.target_local_ipc_addr,
            self.source_node_ip,
            self.target_node_ip,
            self.source_node_id,
            self.target_node_id,
            self.port
        )
