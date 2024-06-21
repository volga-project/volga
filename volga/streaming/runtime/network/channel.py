from typing import Any, Dict, Optional

ChannelMessage = Dict[str, Any]

IPC_DIR = '/tmp/volga_ipc'


class Channel:
    def __init__(
        self,
        channel_id: str, # should be exec_edge_id?
    ):
        self.channel_id = channel_id


# connects two actors on the same node via a zmq.PAIR ipc connection
class LocalChannel(Channel):

    def __init__(
        self,
        channel_id: str,
        ipc_addr: str,
    ):
        super().__init__(channel_id=channel_id)
        self.ipc_addr = ipc_addr


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


def gen_ipc_addr(job_name: str, channel_id: str, node_id: Optional[str] = None) -> str:
    path = f'ipc://{IPC_DIR}/{job_name}'
    if node_id is None:
        return f'{path}/ipc_{channel_id}'
    else:
        return f'{path}/ipc_{channel_id}_{node_id}'


def ipc_path_from_addr(ipc_addr: str) -> str:
    s = ipc_addr.split('/')
    suff = s[-1]

    # remove 'ipc://' prefix and suffix
    return ipc_addr[6:len(ipc_addr) - len(suff)]
