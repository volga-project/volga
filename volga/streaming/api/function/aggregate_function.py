import datetime
import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Tuple, List, Optional, Any, Dict, Callable
from decimal import Decimal

from pydantic import BaseModel

from volga.streaming.api.function.function import Function
from volga.streaming.api.message.message import Record


class AggregationType(str, enum.Enum):
    MAX = 'max'
    MIN = 'min'
    COUNT = 'count'
    SUM = 'sum'
    AVG = 'avg'


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


class AllAggregateFunction(AggregateFunction):

    @dataclass
    class _Acc:
        aggs: Dict[AggregationType, Decimal]

    def __init__(self, agg_type: AggregationType, agg_on_func: Callable):
        self.agg_on_func = agg_on_func
        self.agg_type = agg_type

    def create_accumulator(self) -> _Acc:
        return self._Acc(aggs={})

    def add(self, record: Record, accumulator: _Acc):
        if AggregationType.MIN == self.agg_type:
            v = self.agg_on_func(record.value)
            if AggregationType.MIN in accumulator.aggs:
                accumulator.aggs[AggregationType.MIN] = min(accumulator.aggs[AggregationType.MIN], v)
            else:
                accumulator.aggs[AggregationType.MIN] = v

        if AggregationType.MAX == self.agg_type:
            v = self.agg_on_func(record.value)
            if AggregationType.MAX in accumulator.aggs:
                accumulator.aggs[AggregationType.MAX] = max(accumulator.aggs[AggregationType.MAX], v)
            else:
                accumulator.aggs[AggregationType.MAX] = v

        if AggregationType.COUNT == self.agg_type or AggregationType.AVG == self.agg_type:
            if AggregationType.COUNT in accumulator.aggs:
                accumulator.aggs[AggregationType.COUNT] += Decimal(1)
            else:
                accumulator.aggs[AggregationType.COUNT] = Decimal(1)

        if AggregationType.SUM == self.agg_type or AggregationType.AVG == self.agg_type:
            v = self.agg_on_func(record.value)
            if AggregationType.SUM in accumulator.aggs:
                accumulator.aggs[AggregationType.SUM] += Decimal(v)
            else:
                accumulator.aggs[AggregationType.SUM] = Decimal(v)

            if AggregationType.AVG == self.agg_type:
                accumulator.aggs[AggregationType.AVG] = accumulator.aggs[AggregationType.SUM]/accumulator.aggs[AggregationType.COUNT]

    def get_result(self, accumulator: Any) -> Any:
        return accumulator.aggs

    def merge(self, acc1: Any, acc2: Any) -> Any:
        raise NotImplementedError()








