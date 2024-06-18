import time
from typing import Any

import ray
from UltraDict import UltraDict
from shared_memory_dict import SharedMemoryDict
from multiprocessing import Lock
from atomics import atomic
import locket

@ray.remote
class A:

    def __init__(self, mem_name: str):
        self.d = SharedMemoryDict(name=mem_name, size=1024)

    def put_d(self, k, v):
        self.d[k] = v

    def get_d(self, k) -> Any:
        v = self.d[k]
        return v

with ray.init(address='auto'):

    shmd = 'test_shmd_1'
    a = A.remote(mem_name=shmd)
    b = A.remote(mem_name=shmd)

    ray.get(a.put_d.remote(k='k', v={'k1': 'v'}))
    t = time.time()
    for i in range(100000):
        ray.get(b.get_d.remote(k='k'))
    print(time.time() - t)


# lock = locket.lock_file('/tmp/lock_test')
# lck = Lock()
#
# t = time.time()
# for i in range(1000000):
#     lck.acquire()
#     lck.release()
# print(time.time() - t)