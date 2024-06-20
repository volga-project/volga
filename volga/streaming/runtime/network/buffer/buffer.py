from collections import deque
from typing import List, Dict, Tuple

import simplejson
from pydantic import BaseModel

from volga.streaming.runtime.network.channel import Channel
from volga.streaming.runtime.network.utils import str_to_bytes, int_to_bytes, bytes_to_int, bytes_to_str

Buffer = bytes
# Buffer schema
# [16-channel_id][32-buff_id][32-buff_payload_size][[32-msg_id_0][32-msg_size_0][24 msg-bytes...]... * n_messages]

# buffer header flags
CHANNEL_ID_HEADER_LENGTH = 16  # 16 chars max for channel_id
BUFFER_ID_HEADER_LENGTH = 32  # int
BUFFER_PAYLOAD_SIZE_HEADER_LENGTH = 32  # int
MSG_ID_HEADER_LENGTH = 32  # int
MSG_SIZE_HEADER_LENGTH = 32  # int

DEFAULT_BUFFER_SIZE = 32 * 1024


class AckMessage(BaseModel):
    buffer_id: int
    channel_id: str

    @staticmethod
    def de(raw: str) -> 'AckMessage':
        return AckMessage(**simplejson.loads(raw))

    def ser(self) -> str:
        return simplejson.dumps(self.dict())


class AckMessageBatch(BaseModel):
    acks: List[AckMessage]

    @staticmethod
    def de(raw: str) -> 'AckMessageBatch':
        return AckMessageBatch(**simplejson.loads(raw))

    def ser(self) -> str:
        return simplejson.dumps(self.dict())


class BufferCreator:
    def __init__(self, channels: List[Channel], buffer_queues: Dict[str, deque], buffer_size: int = DEFAULT_BUFFER_SIZE):
        self._buffer_size = buffer_size
        self._msg_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_id_seq = {c.channel_id: 0 for c in channels}
        self._buffer_queues = buffer_queues

    def msg_to_buffers(self, msg: str, channel_id: str) -> List[Buffer]:
        msg_id = self._msg_id_seq[channel_id]
        buffer_id = self._buffer_id_seq[channel_id]

        buffer = serialize(channel_id, buffer_id, msg, msg_id, self._buffer_size)

        # increment sequence numbers
        self._msg_id_seq[channel_id] += 1
        self._buffer_id_seq[channel_id] += 1

        return [buffer]


# TODO move to separate class
# serialization
# TODO implement multi-message buffering
def serialize(channel_id: str, buffer_id: int, msg: str, msg_id: int, buffer_size: int) -> Buffer:
    msg_bytes = str_to_bytes(msg)
    msg_size = len(msg_bytes)
    if msg_size + CHANNEL_ID_HEADER_LENGTH + BUFFER_ID_HEADER_LENGTH + BUFFER_PAYLOAD_SIZE_HEADER_LENGTH + \
            MSG_ID_HEADER_LENGTH + MSG_SIZE_HEADER_LENGTH > buffer_size:
        # TODO implement message splitting
        raise RuntimeError('Message too big')

    msg_id_bytes = int_to_bytes(msg_id, MSG_ID_HEADER_LENGTH)
    buffer_id_bytes = int_to_bytes(buffer_id, BUFFER_ID_HEADER_LENGTH)
    channel_id_bytes = str_to_bytes(channel_id, pad_to_size=CHANNEL_ID_HEADER_LENGTH)

    # TODO implement multi-message buffering
    msg_size_bytes = int_to_bytes(msg_size, MSG_SIZE_HEADER_LENGTH)
    payload_size = MSG_ID_HEADER_LENGTH + MSG_SIZE_HEADER_LENGTH + len(msg_bytes)
    payload_size_bytes = int_to_bytes(payload_size, BUFFER_PAYLOAD_SIZE_HEADER_LENGTH)

    # order must be preserved
    buffer = channel_id_bytes + buffer_id_bytes + payload_size_bytes + msg_id_bytes + msg_size_bytes + msg_bytes

    return buffer


# deser
def get_channel_id(buffer: Buffer) -> str:
    offset = 0
    channel_id_bytes = buffer[offset: offset + CHANNEL_ID_HEADER_LENGTH]
    return bytes_to_str(channel_id_bytes, strip_padding=True)


def get_buffer_id(buffer: Buffer) -> int:
    offset = CHANNEL_ID_HEADER_LENGTH
    buffer_id_bytes = buffer[offset:offset + BUFFER_ID_HEADER_LENGTH]
    return bytes_to_int(buffer_id_bytes)



def get_payload(buffer: Buffer) -> List[Tuple[int, bytes]]: # [(msg_id, msg_bytes), ...]
    offset = CHANNEL_ID_HEADER_LENGTH + BUFFER_ID_HEADER_LENGTH
    payload_size_bytes = buffer[offset: offset + BUFFER_PAYLOAD_SIZE_HEADER_LENGTH]
    payload_size = bytes_to_int(payload_size_bytes)

    offset += BUFFER_PAYLOAD_SIZE_HEADER_LENGTH
    payload_end = offset + payload_size
    res = []
    while offset < payload_end:
        msg_id_bytes = buffer[offset: offset + MSG_ID_HEADER_LENGTH]
        msg_id = bytes_to_int(msg_id_bytes)

        offset += MSG_ID_HEADER_LENGTH
        msg_size_bytes = buffer[offset: offset + MSG_SIZE_HEADER_LENGTH]
        offset += MSG_SIZE_HEADER_LENGTH
        msg_size = bytes_to_int(msg_size_bytes)
        msg_payload_bytes = buffer[offset: offset + msg_size]
        offset += msg_size

        res.append((msg_id, msg_payload_bytes))

    return res



