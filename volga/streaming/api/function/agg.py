import datetime
import enum
from abc import abstractmethod
from typing import Tuple, List, Optional, Any, Dict, Callable
from decimal import Decimal

from pydantic import BaseModel

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


class AllAggregateFunction(AggregateFunction):

    class _Acc(BaseModel):
        aggs: Dict[AggregationType, Decimal]

    def __init__(self, agg_on_funcs: Dict[AggregationType, Callable]):
        self.agg_on_funcs = agg_on_funcs

    def create_accumulator(self) -> _Acc:
        return self._Acc(
            aggs={
                agg_type: None
                for agg_type in self.agg_on_funcs
            }
        )

    def add(self, record: Record, accumulator: _Acc):
        pass









