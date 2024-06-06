

DEFAULT_CAPACITY = 1024 * 32

# TODO make thread safe
class BufferPool:

    def __init__(self, capacity_bytes: int = DEFAULT_CAPACITY):
        self.capacity_bytes = capacity_bytes

    def can_acquire(self, amount: int) -> bool:
        return True

    def acquire(self, amount: int):
        pass

    def release(self, amount: int):
        pass
