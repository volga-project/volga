import dataclasses
from dataclasses import dataclass
from typing import List

import simplejson

from volga.streaming.runtime.transfer.channel import Channel

Buffer = bytes # [32-buff_id][32-msg_id][100-channel_id][32-data_length][rest-...data...]


@dataclass
class AckMessage:

    buff_id: int
    msg_id: int
    channel_id: str

    @staticmethod
    def de(raw: str) -> 'AckMessage':
        return simplejson.loads(raw)

    def ser(self) -> str:
        return simplejson.dumps(dataclasses.asdict(self))


class BufferCreator:
    def __init__(self, channels: List[Channel]):
        self._msg_id_increments = {c.channel_id: 0 for c in channels}
        self._buffer_id_increments = {c.channel_id: 0 for c in channels}


    def msg_to_buffers(self, msg: str, channel_id: str) -> List[Buffer]:
        # TODO append with buff_id, msg_id, channel_id
        return [msg.encode('utf-8')]


def buffer_id(buffer: Buffer) -> int:
    return 0


def buffer_size(buffer: Buffer) -> int:
    return 0


def data_size(buffer: Buffer) -> int:
    return 0


def msg_id(buffer: Buffer) -> int:
    return 0


def channel_id(buffer: Buffer) -> str:
    return ''

