from typing import Any, Dict

ChannelMessage = Dict[str, Any]


class Channel:
    def __init__(
        self,
        channel_id: str, # should be exec_edge_id?
        source_ip: str,
        source_port: int,
    ):
        self.channel_id = channel_id
        self.source_ip = source_ip
        self.source_port = source_port


