import hashlib
import os
from typing import Optional

import locket
from shared_memory_dict import SharedMemoryDict

# each input channel has it's own capacity allocation (in buffers, since we have multiple input queues on DataWriter)
DEFAULT_CAPACITY_PER_IN_CHANNEL = 30 # approx 3 buffers

# for operator's outputs we share capacity per operator (not per channel, since we have one output queue on DataReader)
DEFAULT_CAPACITY_PER_OUT = 20


# TODO add floating buffers
# multiproc memory allocator for buffers
# keeps track of used memory per channel
# uses shared memory to store state + file-based locking
class BufferMemoryTracker:

    _instances = {}

    DICT_MEMORY_SIZE = 1024
    LOCKET_LOCK_DIR = '/tmp/volga_locks'

    @staticmethod
    def instance(
        node_id: str,
        job_name: str,
        capacity_per_in_channel: int = DEFAULT_CAPACITY_PER_IN_CHANNEL,
        capacity_per_out: int = DEFAULT_CAPACITY_PER_OUT
    ) -> 'BufferMemoryTracker':
        key = f'{node_id}-{job_name}'
        lock = BufferMemoryTracker._get_lock(node_id, job_name)
        lock.acquire()
        if key not in BufferMemoryTracker._instances:
            BufferMemoryTracker._instances[key] = BufferMemoryTracker(node_id, job_name, capacity_per_in_channel, capacity_per_out)
        res = BufferMemoryTracker._instances[key]
        lock.release()
        return res

    @staticmethod
    def _get_lock(node_id: str, job_name: str):
        locket_dir = f'{BufferMemoryTracker.LOCKET_LOCK_DIR}/{job_name}'
        os.makedirs(locket_dir, exist_ok=True)
        lock_name = f'{locket_dir}/{node_id}'
        return locket.lock_file(lock_name)

    def __init__(self, node_id: str, job_name: str, capacity_per_channel: int, capacity_per_out: int):
        self._capacity_per_in_channel = capacity_per_channel
        self._capacity_per_out = capacity_per_out
        self._node_id = node_id
        self._job_name = job_name
        self._shared_dict_name = _short_hash(f'{self._node_id}-{self._job_name}')

        locket_dir = f'{BufferMemoryTracker.LOCKET_LOCK_DIR}/{self._job_name}'
        os.makedirs(locket_dir, exist_ok=True)
        self._lock_name = f'{locket_dir}/{self._node_id}'
        self._shared_dict = SharedMemoryDict(name=self._shared_dict_name, size=BufferMemoryTracker.DICT_MEMORY_SIZE)
        self._lock = BufferMemoryTracker._get_lock(node_id, job_name)

    def try_acquire(self, key: str, amount: int = 1, _in: bool = True) -> bool:
        self._lock.acquire()
        v = self._shared_dict.get(key, None)
        if v is None:
            remaining = self._capacity_per_in_channel if _in else self._capacity_per_out
        else:
            remaining = v

        res = amount <= remaining
        if res:
            self._shared_dict[key] = remaining - amount
        self._lock.release()

        return res

    def release(self, key: str, amount: int = 1, _in: bool = True):
        self._lock.acquire()
        remaining = self._shared_dict.get(key, None)
        if remaining is None:
            raise RuntimeError('Releasing non-existing key')
        self._shared_dict[key] = remaining + amount
        self._lock.release()

    def get_capacity(self, key) -> Optional[int]:
        self._lock.acquire()
        res = self._shared_dict.get(key, None)
        self._lock.release()
        return res

    def close(self):
        # TODO cleanup lock file
        self._shared_dict.shm.close()
        self._shared_dict.shm.unlink()


# TODO util this
def _short_hash(s: str, length: int = 16) -> str:
    s = s.encode('utf-8')
    if length < len(hashlib.sha256(s).hexdigest()):
        return hashlib.sha256(s).hexdigest()[:length]
    else:
        raise Exception('Length too long. Length of {y} when hash length is {x}.'.format(
            x=str(len(hashlib.sha256(s).hexdigest())), y=length))