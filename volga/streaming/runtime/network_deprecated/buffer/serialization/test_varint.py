import unittest

from volga.streaming.runtime.network_deprecated.buffer.serialization.varint import encode_int, decode_bytes


class TestVarint(unittest.TestCase):

    def test(self):
        i1 = 1024
        b1 = encode_int(i1)
        v, pos = decode_bytes(b1, 0)
        assert i1 == v
        assert pos == len(b1)

        i2 = 1024
        b2 = encode_int(i2)
        bef = b'abcd'
        aft = b'efgh'

        b = bef + b1 + b2 + aft
        v1, pos1 = decode_bytes(b, len(bef))
        assert v1 == i1
        v2, pos2 = decode_bytes(b, pos1)
        assert v2 == i2
        assert aft == b[pos2:]


if __name__ == '__main__':
    t = TestVarint()
    t.test()