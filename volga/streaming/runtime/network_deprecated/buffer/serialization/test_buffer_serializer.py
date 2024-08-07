import datetime
import unittest

from volga.streaming.runtime.network_deprecated.buffer.serialization.buffer_serializer import BufferSerializer, \
    JSONBufferSerializer
from volga.streaming.runtime.network_deprecated.buffer.serialization.byte_utils import bytes_to_str, str_to_bytes


class TestBufferSerializer(unittest.TestCase):

    def test_utils(self):

        s = 'acbdefgh'
        assert s == bytes_to_str(str_to_bytes(s))
        assert s == bytes_to_str(str_to_bytes(s, pad_to_size=10), strip_padding=True)
        print('assert ok')

    def test_buffer_serializer(self):
        BUFFER_SIZE = 32 * 1024

        channel_id = '1234_5678'
        msg_id = 1
        buffer_id = 1
        msg_bytes = b'abcd'
        buffer = BufferSerializer.serialize_msg_bytes(channel_id, buffer_id, msg_bytes, msg_id, BUFFER_SIZE, with_header=True)

        assert channel_id == BufferSerializer.get_channel_id(buffer)
        assert buffer_id == BufferSerializer.get_buffer_id(buffer)
        payload = BufferSerializer.get_payload(buffer)
        assert len(payload) == 1
        _msg_id, _msg_bytes = payload[0]
        assert msg_id == _msg_id
        assert msg_bytes == _msg_bytes
        print('assert ok')

    def test_append(self):
        BUFFER_SIZE = 32 * 1024

        channel_id = '1234_5678'
        buffer_id = 1

        msg_id_1 = 1
        msg_1_bytes = b'abc'
        buffer = BufferSerializer.serialize_msg_bytes(channel_id, buffer_id, msg_1_bytes, msg_id_1, BUFFER_SIZE, with_header=True)

        msg_id_2 = 2
        msg_2_bytes = b'def'
        payload_2 = BufferSerializer.serialize_msg_bytes(channel_id, buffer_id, msg_2_bytes, msg_id_2, BUFFER_SIZE, with_header=False)

        new_buffer = BufferSerializer.append_msg_bytes(buffer, payload_2, BUFFER_SIZE)
        assert channel_id == BufferSerializer.get_channel_id(new_buffer)
        assert buffer_id == BufferSerializer.get_buffer_id(new_buffer)
        payload = BufferSerializer.get_payload(new_buffer)
        assert len(payload) == 2
        _msg_id_1, _msg_bytes_1 = payload[0]
        _msg_id_2, _msg_bytes_2 = payload[1]
        assert msg_id_1 == _msg_id_1
        assert msg_1_bytes == _msg_bytes_1
        assert msg_id_2 == _msg_id_2
        assert msg_2_bytes == _msg_bytes_2
        print('assert ok')

    def test_json_serializer(self):
        data = {'i': 123456, 'm': {'k': 'abc'}, 'date': str(datetime.datetime.now())}
        assert data == JSONBufferSerializer.bytes_to_msg(JSONBufferSerializer.msg_to_bytes(data))
        print('assert ok')


if __name__ == '__main__':
    t = TestBufferSerializer()
    t.test_utils()
    t.test_buffer_serializer()
    t.test_append()
    t.test_json_serializer()
