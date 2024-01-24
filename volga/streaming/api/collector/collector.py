import logging
from abc import ABC, abstractmethod
from typing import Any, List

from volga.streaming.api.message.message import Record

logger = logging.getLogger(__name__)


class Collector(ABC):
    # Collects data from an upstream operator and emits to downstream operators

    @abstractmethod
    def collect(self, record: Record):
        pass


class CollectionCollector(Collector):
    def __init__(self, collector_list: List[Collector]):
        self._collector_list = collector_list

    def collect(self, value: Any):
        for collector in self._collector_list:
            collector.collect(Record(value))
