from abc import ABC, abstractmethod
from typing import Union, Tuple, List

import orjson

import volga.streaming.runtime.network.buffer.serialization.varint as vi
from volga.streaming.runtime.network.buffer.buffer import CHANNEL_ID_HEADER_LENGTH, Buffer
from volga.streaming.runtime.network.buffer.serialization.byte_utils import str_to_bytes, bytes_to_str
from volga.streaming.runtime.network.channel import ChannelMessage


class BufferSerializer(ABC):

    @classmethod
    @abstractmethod
    def msg_to_bytes(cls, msg: ChannelMessage) -> bytes:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def bytes_to_msg(cls, b: bytes) -> ChannelMessage:
        raise NotImplementedError

    @classmethod
    def serialize_msg(cls, channel_id: str, buffer_id: int, msg: ChannelMessage, msg_id: int, buffer_size: int, with_header: bool = True) -> Buffer:
        msg_bytes = cls.msg_to_bytes(msg)
        return BufferSerializer.serialize_msg_bytes(channel_id=channel_id, buffer_id=buffer_id, msg_bytes=msg_bytes, msg_id=msg_id, buffer_size=buffer_size, with_header=with_header)

    @classmethod
    def deser(cls, buffer: Buffer) -> List[ChannelMessage]:
        payload = BufferSerializer.get_payload(buffer)
        res = []
        for (_, data) in payload:
            msg = cls.bytes_to_msg(data)
            res.append(msg)
        return res

    @staticmethod
    def serialize_msg_bytes(channel_id: str, buffer_id: int, msg_bytes: bytes, msg_id: int, buffer_size: int, with_header: bool = True) -> Buffer:
        msg_id_bytes = vi.encode_int(msg_id)
        msg_size = len(msg_bytes)
        msg_size_bytes = vi.encode_int(msg_size)
        if with_header:
            buffer_id_bytes = vi.encode_int(buffer_id)
            channel_id_bytes = str_to_bytes(channel_id, pad_to_size=CHANNEL_ID_HEADER_LENGTH)

            payload_size = len(msg_id_bytes) + len(msg_size_bytes) + len(msg_bytes)
            payload_size_bytes = vi.encode_int(payload_size)

            if msg_size + CHANNEL_ID_HEADER_LENGTH + len(buffer_id_bytes) + len(payload_size_bytes) + len(
                    msg_id_bytes) + \
                    len(msg_size_bytes) > buffer_size:
                raise RuntimeError('Message too big')

            # order must be preserved
            buffer = channel_id_bytes + buffer_id_bytes + payload_size_bytes + msg_id_bytes + msg_size_bytes + msg_bytes
        else:
            buffer = msg_id_bytes + msg_size_bytes + msg_bytes

        return buffer

    @staticmethod
    def get_channel_id(buffer: Buffer) -> str:
        offset = 0
        channel_id_bytes = buffer[offset: offset + CHANNEL_ID_HEADER_LENGTH]
        return bytes_to_str(channel_id_bytes, strip_padding=True)

    @staticmethod
    def get_buffer_id(buffer: Buffer, return_offset: bool = False) -> Union[int, Tuple[int, int]]:
        offset = CHANNEL_ID_HEADER_LENGTH
        res = vi.decode_bytes(buffer, offset)
        if return_offset:
            return res
        else:
            return res[0]

    @staticmethod
    def get_payload_size(buffer: Buffer) -> Tuple[int, int]:
        _, offset = BufferSerializer.get_buffer_id(buffer, return_offset=True)
        return vi.decode_bytes(buffer, offset)

    @staticmethod
    def get_payload(buffer: Buffer) -> List[Tuple[int, bytes]]:  # [(msg_id, msg_bytes), ...]
        payload_size, offset = BufferSerializer.get_payload_size(buffer)
        payload_end = offset + payload_size
        res = []
        while offset < payload_end:
            msg_id, offset = vi.decode_bytes(buffer, offset)
            msg_size, offset = vi.decode_bytes(buffer, offset)
            msg_payload_bytes = buffer[offset: offset + msg_size]
            offset += msg_size

            res.append((msg_id, msg_payload_bytes))

        return res

    @staticmethod
    def append_msg_bytes(buffer: Buffer, payload: bytes, buffer_size: int) -> Buffer:
        if len(buffer) + len(payload) > buffer_size:
            raise RuntimeError('Can not append, buffer too big')

        _, old_payload_size_bytes_start = BufferSerializer.get_buffer_id(buffer, return_offset=True)
        payload_size, old_payload_size_bytes_end = BufferSerializer.get_payload_size(buffer)
        new_payload_size = payload_size + len(payload)
        new_payload_size_bytes = vi.encode_int(new_payload_size)

        # construct new buffer
        return buffer[:old_payload_size_bytes_start] + new_payload_size_bytes + buffer[old_payload_size_bytes_end:] + payload


class JSONBufferSerializer(BufferSerializer):

    @classmethod
    def msg_to_bytes(cls, msg: ChannelMessage) -> bytes:
        return orjson.dumps(msg)

    @classmethod
    def bytes_to_msg(cls, b: bytes) -> ChannelMessage:
        return orjson.loads(b)
