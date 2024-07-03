import os
from typing import Optional

import locket
from shared_memory_dict import SharedMemoryDict

# each input channel has it's own capacity allocation (in buffers, since we have multiple input queues on DataWriter)
DEFAULT_CAPACITY_PER_IN_CHANNEL = 3 # approx 3 buffers

# for operator's outputs we share capacity per operator (not per channel, since we have one output queue on DataReader)
DEFAULT_CAPACITY_PER_OUT = 2


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
        capacity_per_in_channel: int = DEFAULT_CAPACITY_PER_IN_CHANNEL,
        capacity_per_out: int = DEFAULT_CAPACITY_PER_OUT
    ) -> 'BufferMemoryTracker':
        if node_id not in BufferMemoryTracker._instances:
            BufferMemoryTracker._instances[node_id] = BufferMemoryTracker(node_id, capacity_per_in_channel, capacity_per_out)
        return BufferMemoryTracker._instances[node_id]

    def __init__(self, node_id: str, capacity_per_channel: int, capacity_per_out: int):
        self._capacity_per_in_channel = capacity_per_channel
        self._capacity_per_out = capacity_per_out
        self._node_id = node_id
        # TODO should we add job_name to dict and lock identifiers?
        self._shared_dict_name = f'shared-dict-{self._node_id}'

        os.makedirs(BufferMemoryTracker.LOCKET_LOCK_DIR, exist_ok=True)
        self._lock_name = f'{BufferMemoryTracker.LOCKET_LOCK_DIR}/lock-memory-allocator-{self._node_id}'
        self._shared_dict = SharedMemoryDict(name=self._shared_dict_name, size=BufferMemoryTracker.DICT_MEMORY_SIZE)
        self._lock = locket.lock_file(self._lock_name)

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
        self._shared_dict.shm.close()
        self._shared_dict.shm.unlink()
