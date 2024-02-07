import unittest

from volga.streaming.api.context.streaming_context import StreamingContext


class TestStream(unittest.TestCase):

    def test_stream(self):
        ctx = StreamingContext()
        stream = ctx.from_values(1, 2, 3)
        stream.set_parallelism(10)
        assert stream.id == 1
        assert stream.parallelism == 10

    def test_key_stream(self):
        ctx = StreamingContext()
        key_stream = ctx\
            .from_values('a', 'b', 'c')\
            .map(lambda x: (x, 1))\
            .key_by(lambda x: x[0])

        key_stream.set_parallelism(10)
        assert key_stream.parallelism == 10


if __name__ == '__main__':
    t = TestStream()
    t.test_stream()
    t.test_key_stream()