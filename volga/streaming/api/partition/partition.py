from abc import ABC, abstractmethod
from typing import Any, List


class Partition(ABC):

    # Interface of the partitioning strategy
    @abstractmethod
    def partition(self, record: Any, num_partition: int) -> List[int]:
        pass


class BroadcastPartition(Partition):
    """Broadcast the record to all downstream partitions."""

    def __init__(self):
        self.__partitions = []

    def partition(self, record: Any, num_partition: int) -> List[int]:
        if len(self.__partitions) != num_partition:
            self.__partitions = list(range(num_partition))
        return self.__partitions


class KeyPartition(Partition):
    """Partition the record by the key."""

    def __init__(self):
        self.__partitions = [-1]

    def partition(self, record: Any, num_partition: int) -> List[int]:
        # TODO support key group
        self.__partitions[0] = abs(hash(record.key)) % num_partition
        return self.__partitions


class RoundRobinPartition(Partition):
    """Partition record to downstream tasks in a round-robin matter."""

    def __init__(self):
        self.__partitions = [-1]
        self.seq = 0

    def partition(self, record: Any, num_partition: int) -> List[int]:
        self.seq = (self.seq + 1) % num_partition
        self.__partitions[0] = self.seq
        return self.__partitions


class ForwardPartition(Partition):
    """Default partition for operator if the operator can be chained with
    succeeding operators."""

    def __init__(self):
        self.__partitions = [0]

    def partition(self, record: Any, num_partition: int) -> List[int]:
        return self.__partitions


class SimplePartition(Partition):
    """Wrap a python function as subclass of :class:`Partition`"""

    def __init__(self, func):
        self.func = func

    def partition(self, record: Any, num_partition: int) -> List[int]:
        return self.func(record, num_partition)
