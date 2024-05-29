from typing import List


# TODO make thread safe
class BufferPool:

    def __init__(self, capacity_bytes: int):
        self.capacity_bytes = capacity_bytes

    def can_acquire(self, amount: int):
        pass

    def acquire(self, amount: int):
        pass

    def release(self, amount: int):
        pass

def msg_to_buffers(s: str) -> List[bytearray]:
    pass
