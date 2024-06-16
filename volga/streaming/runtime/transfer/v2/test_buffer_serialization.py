import unittest

import simplejson

from volga.streaming.runtime.transfer.v2.buffer import serialize, get_buffer_id, get_channel_id, get_payload
from volga.streaming.runtime.transfer.v2.utils import bytes_to_str, int_to_bytes, bytes_to_int, str_to_bytes


class TestBufferSerialization(unittest.TestCase):

    def test_utils(self):
        i = 1234
        assert i == bytes_to_int(int_to_bytes(i, 32*1024))

        s = 'acbdefgh'
        assert s == bytes_to_str(str_to_bytes(s))
        assert s == bytes_to_str(str_to_bytes(s, pad_to_size=10), strip_padding=True)


    def test_ser_de(self):
        BUFFER_SIZE = 32 * 1024

        channel_id = '1234_5678'
        msg_id = 1
        buffer_id = 1
        data = {'test_key': 'test_val'}
        msg = simplejson.dumps(data)
        buffer = serialize(channel_id, buffer_id, msg, msg_id, BUFFER_SIZE)

        assert channel_id == get_channel_id(buffer)
        assert buffer_id == get_buffer_id(buffer)
        payload = get_payload(buffer)
        assert len(payload) == 1
        _msg_id, _msg_bytes = payload[0]
        assert msg_id == _msg_id
        assert data == simplejson.loads(bytes_to_str(_msg_bytes))


if __name__ == '__main__':
    t = TestBufferSerialization()
    t.test_utils()
    t.test_ser_de()
