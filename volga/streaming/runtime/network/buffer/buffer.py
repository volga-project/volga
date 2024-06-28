from typing import List, Tuple

import simplejson
from pydantic import BaseModel

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


# TODO implement multimessage buffering
def serialize(channel_id: str, buffer_id: int, msg: str, msg_id: int, buffer_size: int, with_header: bool = True) -> bytes:
    msg_bytes = str_to_bytes(msg)
    msg_id_bytes = int_to_bytes(msg_id, MSG_ID_HEADER_LENGTH)
    msg_size = len(msg_bytes)
    msg_size_bytes = int_to_bytes(msg_size, MSG_SIZE_HEADER_LENGTH)
    if with_header:
        if msg_size + CHANNEL_ID_HEADER_LENGTH + BUFFER_ID_HEADER_LENGTH + BUFFER_PAYLOAD_SIZE_HEADER_LENGTH + \
                MSG_ID_HEADER_LENGTH + MSG_SIZE_HEADER_LENGTH > buffer_size:
            # TODO implement message splitting
            raise RuntimeError('Message too big')

        buffer_id_bytes = int_to_bytes(buffer_id, BUFFER_ID_HEADER_LENGTH)
        channel_id_bytes = str_to_bytes(channel_id, pad_to_size=CHANNEL_ID_HEADER_LENGTH)

        payload_size = MSG_ID_HEADER_LENGTH + MSG_SIZE_HEADER_LENGTH + len(msg_bytes)
        payload_size_bytes = int_to_bytes(payload_size, BUFFER_PAYLOAD_SIZE_HEADER_LENGTH)

        # order must be preserved
        buffer = channel_id_bytes + buffer_id_bytes + payload_size_bytes + msg_id_bytes + msg_size_bytes + msg_bytes
    else:
        buffer = msg_id_bytes + msg_size_bytes + msg_bytes

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


def get_payload_size_bytes(buffer: Buffer) -> int:
    offset = CHANNEL_ID_HEADER_LENGTH + BUFFER_ID_HEADER_LENGTH
    payload_size_bytes = buffer[offset: offset + BUFFER_PAYLOAD_SIZE_HEADER_LENGTH]
    payload_size = bytes_to_int(payload_size_bytes)
    return payload_size


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


def append_to_buffer(buffer: Buffer, payload: bytes, buffer_size: int) -> Buffer:
    if len(buffer) + len(payload) > buffer_size:
        raise RuntimeError('Can not append, buffer too big')

    payload_size = get_payload_size_bytes(buffer)
    new_payload_size = payload_size + len(payload)
    new_payload_size_bytes = int_to_bytes(new_payload_size, BUFFER_PAYLOAD_SIZE_HEADER_LENGTH)
    new_buff = bytearray(buffer + payload)
    # update payload size
    offset = CHANNEL_ID_HEADER_LENGTH + BUFFER_ID_HEADER_LENGTH
    new_buff[offset: offset + BUFFER_PAYLOAD_SIZE_HEADER_LENGTH] = new_payload_size_bytes
    return bytes(new_buff)


class AckMessage(BaseModel):
    buffer_id: int


class AckMessageBatch(BaseModel):
    channel_id: str
    acks: List[AckMessage]

    @staticmethod
    def de(raw: bytes) -> 'AckMessageBatch':
        # remove channel_id header
        payload = bytearray(raw)[CHANNEL_ID_HEADER_LENGTH:]
        return AckMessageBatch(**simplejson.loads(payload.decode()))

    def ser(self) -> bytes:
        str = simplejson.dumps(self.dict())
        data = str.encode('utf-8')

        # append channel_id header
        channel_id_bytes = str_to_bytes(self.channel_id, pad_to_size=CHANNEL_ID_HEADER_LENGTH)

        return channel_id_bytes + data