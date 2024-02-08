import time
import unittest
from typing import List, Dict, Any

from volga.streaming.runtime.transfer.channel import Channel
from volga.streaming.runtime.transfer.data_reader import DataReader
from volga.streaming.runtime.transfer.data_writer import DataWriter

import ray

TERMINAL_MESSAGE = {'data': 'done'}


@ray.remote
class Writer:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_writer = DataWriter(name='test_writer', source_stream_name='0', output_channels=[channel])

    def send_items(self, items: List[Dict]):
        for item in items:
            self.data_writer.write_message(self.channel.channel_id, item)
        self.data_writer.write_message(self.channel.channel_id, TERMINAL_MESSAGE)


@ray.remote
class Reader:
    def __init__(
        self,
        channel: Channel,
    ):
        self.channel = channel
        self.data_reader = DataReader(name='test_reader', input_channels=[channel])

    def receive_items(self) -> List[Any]:
        t = time.time()
        res = []
        while True:
            if time.time() - t > 10:
                raise RuntimeError('Timeout waiting for data')

            item = self.data_reader.read_message()
            if item == TERMINAL_MESSAGE:
                break
            else:
                res.append(item)
        return res


class TestTransfer(unittest.TestCase):

    def test_one_to_one_transfer(self):
        channel = Channel(
            channel_id='1',
            source_ip='127.0.0.1',
            source_port=4321
        )
        ray.init()
        num_items = 10000
        items = [{
            'data': f'{i}'
        } for i in range(num_items)]

        reader = Reader.remote(channel)
        writer = Writer.remote(channel)

        # make sure Ray has enough time to start actors
        time.sleep(1)
        writer.send_items.remote(items)
        received = ray.get(reader.receive_items.remote())

        assert items == received

        ray.shutdown()


if __name__ == '__main__':
    t = TestTransfer()
    t.test_one_to_one_transfer()