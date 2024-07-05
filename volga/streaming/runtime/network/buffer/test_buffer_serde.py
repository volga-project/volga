import unittest

import simplejson

from volga.streaming.runtime.network.buffer.buffer import serialize, get_buffer_id, get_channel_id, get_payload, \
    append_to_buffer
from volga.streaming.runtime.network.byte_utils import bytes_to_str, int_to_bytes, bytes_to_int, str_to_bytes


class TestBufferSerialization(unittest.TestCase):

    def test_utils(self):
        i = 1234
        assert i == bytes_to_int(int_to_bytes(i, 32*1024))

        s = 'acbdefgh'
        assert s == bytes_to_str(str_to_bytes(s))
        assert s == bytes_to_str(str_to_bytes(s, pad_to_size=10), strip_padding=True)
        print('assert ok')

    def test_ser_de(self):
        BUFFER_SIZE = 32 * 1024

        channel_id = '1234_5678'
        msg_id = 1
        buffer_id = 1
        data = {'test_key': 'test_val'}
        msg = simplejson.dumps(data)
        buffer = serialize(channel_id, buffer_id, msg, msg_id, BUFFER_SIZE, with_header = True)

        assert channel_id == get_channel_id(buffer)
        assert buffer_id == get_buffer_id(buffer)
        payload = get_payload(buffer)
        assert len(payload) == 1
        _msg_id, _msg_bytes = payload[0]
        assert msg_id == _msg_id
        assert data == simplejson.loads(bytes_to_str(_msg_bytes))
        print('assert ok')

    def test_append(self):
        BUFFER_SIZE = 32 * 1024

        channel_id = '1234_5678'
        buffer_id = 1

        msg_id_1 = 1
        data_1 = {'test_key_1': 'test_val_1'}
        msg_1 = simplejson.dumps(data_1)
        buffer = serialize(channel_id, buffer_id, msg_1, msg_id_1, BUFFER_SIZE, with_header=True)

        msg_id_2 = 2
        data_2 = {'test_key_2': 'test_val_2'}
        msg_2 = simplejson.dumps(data_2)
        payload_2 = serialize(channel_id, buffer_id, msg_2, msg_id_2, BUFFER_SIZE, with_header=False)

        new_buffer = append_to_buffer(buffer, payload_2, BUFFER_SIZE)
        assert channel_id == get_channel_id(new_buffer)
        assert buffer_id == get_buffer_id(new_buffer)
        payload = get_payload(new_buffer)
        assert len(payload) == 2
        _msg_id_1, _msg_bytes_1 = payload[0]
        _msg_id_2, _msg_bytes_2 = payload[1]
        assert msg_id_1 == _msg_id_1
        assert data_1 == simplejson.loads(bytes_to_str(_msg_bytes_1))
        assert msg_id_2 == _msg_id_2
        assert data_2 == simplejson.loads(bytes_to_str(_msg_bytes_2))
        print('assert ok')

    def test_append_many(self):
        num_items = 10000
        buffer_size = 32*1024
        channel_id='1'
        to_send = [{'i': i} for i in range(num_items)]
        r = []
        msg_id = 0
        buff_id = 0
        for msg in to_send:
            msg_str = simplejson.dumps(msg)
            if len(r) == 0:
                buff = serialize(channel_id, buff_id, msg_str, msg_id, buffer_size, with_header=True)
                r.append(buff)
                buff_id += 1
                msg_id += 1
                continue

            payload = serialize(channel_id, buff_id, msg_str, msg_id, buffer_size, with_header=False)
            try:
                buff = append_to_buffer(r[-1], payload, buffer_size)
                r[-1] = buff
            except:
                buff = serialize(channel_id, buff_id, msg_str, msg_id, buffer_size, with_header=True)
                r.append(buff)
                buff_id += 1
            msg_id += 1

        res = []
        last_msg_id = -1
        for buffer in r:
            payload = get_payload(buffer)

            for (msg_id, data) in payload:
                if msg_id != last_msg_id + 1:
                    raise RuntimeError(f'msg_id order missmatch')
                last_msg_id = msg_id
                msg = simplejson.loads(bytes_to_str(data))
                if msg_id != msg['i']:
                    raise RuntimeError('msg_id missmatch')
                res.append(msg)

        assert to_send == res
        print('assert ok')



if __name__ == '__main__':
    t = TestBufferSerialization()
    t.test_utils()
    t.test_ser_de()
    t.test_append()
    t.test_append_many()
