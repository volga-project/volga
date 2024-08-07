from typing import Any, Dict

ChannelMessage = Dict[str, Any]


class BiChannel:
    def __init__(
        self,
        channel_id: str, # should be exec_edge_id?
    ):
        self.channel_id = channel_id


# connects two actors on the same node via a simple ZMQ ipc connection
class LocalBiChannel(BiChannel):

    def __init__(
        self,
        channel_id: str,
        ipc_addr_out: str,
        ipc_addr_in: str
    ):
        super().__init__(channel_id=channel_id)
        self.ipc_addr_out = ipc_addr_out
        self.ipc_addr_in = ipc_addr_in


# connects two actors on dofferent nodes
# 3-part channel: local ipc on source, source <-> target TCP, local ipc on target
class RemoteBiChannel(BiChannel):

    def __init__(
        self,
        channel_id: str,
        source_local_ipc_addr_out: str,
        source_local_ipc_addr_in: str,
        source_node_ip: str,
        source_node_id: str,
        target_local_ipc_addr_out: str,
        target_local_ipc_addr_in: str,
        target_node_ip: str,
        target_node_id: str,
        port_out: int,
        port_in: int
    ):
        super().__init__(channel_id=channel_id)
        self.source_local_ipc_addr_out = source_local_ipc_addr_out
        self.source_local_ipc_addr_in = source_local_ipc_addr_in
        self.target_local_ipc_addr_out = target_local_ipc_addr_out
        self.target_local_ipc_addr_in = target_local_ipc_addr_in
        self.source_node_ip = source_node_ip
        self.target_node_ip = target_node_ip
        self.source_node_id = source_node_id
        self.target_node_id = target_node_id
        self.port_out = port_out
        self.port_in = port_in

