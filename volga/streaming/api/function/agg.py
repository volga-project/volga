import datetime
import enum
from abc import abstractmethod
from typing import Tuple, List, Optional, Any
from decimal import Decimal

from volga.streaming.api.function.function import Function
from volga.streaming.api.message.message import Record


class AggregationType(enum.Enum):
    MAX = 'max'
    MIN = 'min'
    AVG = 'avg'
    COUNT = 'count'
    SUM = 'sum'
    STDDEV = 'stddev'


class AggregateFunction(Function):

    @abstractmethod
    def create_accumulator(self) -> Any:
        # creates stateful object
        pass

    @abstractmethod
    def add(self, record: Record, accumulator: Any):
        # updates accumulator for every event
        pass

    @abstractmethod
    def get_result(self, accumulator: Any) -> Any:
        # returns aggregation result based on accumulator state
        pass

    @abstractmethod
    def merge(self, acc1: Any, acc2: Any) -> Any:
        # merges 2 accumulators into 1
        pass

    @abstractmethod
    def retract(self, record: Record, accumulator: Any):
        # updates accumulator for event removal, for example in windowed aggregations
        pass



