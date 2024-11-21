from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, List



class Partition(ABC):

    # Interface of the partitioning strategy
    @abstractmethod
    def partition(self, record: Any, num_partition: int) -> List[int]:
        pass


class BroadcastPartition(Partition):
    """Broadcast the record to all downstream partitions."""

    def __init__(self):
        self._partitions = []

    def partition(self, record: Any, num_partition: int) -> List[int]:
        if len(self._partitions) != num_partition:
            self._partitions = list(range(num_partition))
        return self._partitions


class KeyPartition(Partition):
    """Partition the record by the key."""

    def __init__(self):
        self._partitions = [-1]

    @lru_cache(maxsize=1024) # TODO check if this has any effect on perf
    def _hash(self, key: Any) -> int:
        _hash = abs(hash(key)) # TODO measure perf overhead - this is the best so far
        # _hash = xxhash.xxh64(record.key).intdigest() # TODO this is slightly slower (10%)
        # _hash = int(joblib.hash(record.key, hash_name='md5'), base=16) # TODO this is 2x slower
        return _hash

    # make sure we set PYTHONHASHSEED same across all workers
    def partition(self, record: Any, num_partition: int) -> List[int]:
        # TODO support key group
        _hash = self._hash(record.key)
        self._partitions[0] = _hash % num_partition
        return self._partitions


class RoundRobinPartition(Partition):
    """Partition record to downstream tasks in a round-robin matter."""

    def __init__(self):
        self._partitions = [-1]
        self.seq = 0

    def partition(self, record: Any, num_partition: int) -> List[int]:
        self.seq = (self.seq + 1) % num_partition
        self._partitions[0] = self.seq
        return self._partitions


class ForwardPartition(Partition):
    """Default partition for operator if the operator can be chained with
    succeeding operators."""

    def __init__(self):
        self._partitions = [0]

    def partition(self, record: Any, num_partition: int) -> List[int]:
        return self._partitions


class SimplePartition(Partition):
    """Wrap a python function as subclass of :class:`Partition`"""

    def __init__(self, func):
        self.func = func

    def partition(self, record: Any, num_partition: int) -> List[int]:
        return self.func(record, num_partition)
