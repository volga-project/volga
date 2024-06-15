

DEFAULT_CAPACITY = 1024 * 32


# TODO make thread/proc safe
class BufferPool:

    _instance = None

    @staticmethod
    def instance(node_id: str, capacity_bytes: int = DEFAULT_CAPACITY):
        if BufferPool._instance is None:
            BufferPool._instance = BufferPool(node_id, capacity_bytes)
        return BufferPool._instance

    def __init__(self, node_id: str, capacity_bytes: int = DEFAULT_CAPACITY):
        self._capacity_bytes = capacity_bytes
        self._node_id = node_id

    def try_acquire(self, amount: int) -> bool:
        return True

    def release(self, amount: int):
        pass
