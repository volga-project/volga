import time
import unittest
import random
from threading import Thread

import simplejson

from volga.streaming.runtime.network.buffer.buffer import get_buffer_id, get_payload
from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker
from volga.streaming.runtime.network.buffer.buffer_queues import BufferQueues
from volga.streaming.runtime.network.buffer.buffering_policy import PeriodicPartialFlushPolicy, BufferPerMessagePolicy, \
    BufferingPolicy
from volga.streaming.runtime.network.channel import LocalChannel
from volga.streaming.runtime.network.utils import bytes_to_str


class TestBufferQueues(unittest.TestCase):

    def test(self, num_items: int, buffer_size: int, buffering_policy: BufferingPolicy):
        channel = LocalChannel(
            channel_id='1',
            ipc_addr='ipc:///tmp/zmqtest',
        )

        to_send = [{'i': i} for i in range(num_items)]
        bma = BufferMemoryTracker.instance(node_id='1', capacity_per_in_channel=3, capacity_per_out=2)
        queues = BufferQueues(
            channels=[channel],
            buffer_memory_tracker=bma,
            buffer_size=buffer_size,
            buffering_policy=buffering_policy
        )
        running = [True]
        rcvd = []
        def write():
            last_msg_id = -1
            for msg in to_send:
                res = queues.append_msg(msg, channel_id=channel.channel_id)
                while running[0] and res is None:
                    res = queues.append_msg(msg, channel_id=channel.channel_id)
                    time.sleep(0.001)
                msg_id = res[0]
                if msg_id != last_msg_id + 1:
                    running[0] = False
                    raise RuntimeError(f'Write msg_id unordered: {last_msg_id}, {msg_id}')
                last_msg_id = msg_id
                if not running[0]:
                    break

        def send_and_receive():
            timeout = 100
            t = time.time()
            last_msg_id = -1
            while running[0]:
                if time.time() - t > timeout:
                    raise RuntimeError('Timeout')
                buffer = queues.schedule_next(channel_id=channel.channel_id)
                if buffer is None:
                    time.sleep(0.001)
                    continue
                payload = get_payload(buffer)

                for (msg_id, data) in payload:
                    if msg_id != last_msg_id + 1:
                        running[0] = False
                        raise RuntimeError(f'Read msg_id unordered: {last_msg_id}, {msg_id}, {data}')
                    last_msg_id = msg_id
                    msg = simplejson.loads(bytes_to_str(data))
                    if msg_id != msg['i']:
                        raise RuntimeError('msg_id missmatch')
                    rcvd.append(msg)

                time.sleep(random.uniform(0.001, 0.002))
                queues.pop(channel_id=channel.channel_id, buffer_id=get_buffer_id(buffer))

                if len(rcvd) == num_items:
                    break

        t = Thread(target=write)
        t.start()
        send_and_receive()
        assert to_send == rcvd
        t.join(5)
        print('assert ok')


if __name__ == '__main__':
    t = TestBufferQueues()
    # test PeriodicPartialFlushPolicy
    t.test(1000000, 132 * 1024, PeriodicPartialFlushPolicy(0.0015))

    # test BufferPerMessagePolicy
    t.test(10000, 32 * 1024, BufferPerMessagePolicy())