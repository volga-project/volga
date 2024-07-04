import unittest
from threading import Thread
import time

from volga.streaming.runtime.network.buffer.buffer_memory_tracker import BufferMemoryTracker


class TestBufferMemoryTracker(unittest.TestCase):

    def test(self):
        job_name = f'job-{int(time.time())}'
        channel_id = 'ch1'
        out_task_name = 'out1'
        bmt1 = BufferMemoryTracker.instance(node_id='1', job_name=job_name, capacity_per_in_channel=2, capacity_per_out=2)
        r1 = bmt1.try_acquire(key=channel_id, amount=2, _in=True)
        assert r1 is True
        assert bmt1.get_capacity(key=channel_id) == 0
        r2 = bmt1.try_acquire(key=channel_id, amount=2, _in=True)
        assert r2 is False
        bmt1.release(key=channel_id, amount=2, _in=True)
        r3 = bmt1.try_acquire(key=channel_id, amount=2, _in=True)
        assert r3 is True
        r4 = bmt1.try_acquire(key=out_task_name, amount=2, _in=False)
        assert r4 is True
        bmt1.release(key=out_task_name, amount=1, _in=False)
        assert bmt1.get_capacity(key=out_task_name) == 1

        # test different key
        r5 = bmt1.try_acquire(key='ch2', amount=2, _in=True)
        assert r5 is True

        # test second to make sure they do not share same memory
        bmt2 = BufferMemoryTracker.instance(node_id='2', job_name=job_name, capacity_per_in_channel=2, capacity_per_out=2)
        r6 = bmt2.try_acquire(key=channel_id, amount=2, _in=True)
        assert r6 is True

        bmt1.close()
        bmt2.close()

        print('assert ok')

    def test_concurrent_access(self):
        job_name = f'job-{int(time.time())}'
        local_bmt = BufferMemoryTracker.instance(node_id='3', job_name=job_name, capacity_per_in_channel=2, capacity_per_out=2)
        key = 'ch1'

        def p1():
            for _ in range(10000):
                res = local_bmt.try_acquire(key=key, amount=2, _in=True)
                if res:
                    local_bmt.release(key=key, amount=2, _in=True)

        def p2():
            for _ in range(10000):
                res = local_bmt.try_acquire(key=key, amount=2, _in=True)
                if res:
                    local_bmt.release(key=key, amount=2, _in=True)

        # We use threads since procs cant pickle certain objects.
        # Scenario is the same, we test concurrency stability and make sure locking works properly
        pr1 = Thread(target=p1)
        pr2 = Thread(target=p2)
        pr1.start()
        pr2.start()
        pr1.join()
        pr2.join()
        assert local_bmt.get_capacity(key) == 2
        local_bmt.close()
        print('assert ok')


if __name__ == '__main__':
    t = TestBufferMemoryTracker()
    t.test()
    t.test_concurrent_access()