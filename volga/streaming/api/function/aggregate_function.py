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
        aggs: Dict[str, Decimal]

    def __init__(self, agg_on_funcs: Dict[str, Callable]):
        self.agg_on_funcs = agg_on_funcs

    def create_accumulator(self) -> _Acc:
        return self._Acc(aggs={})

    def add(self, record: Record, accumulator: _Acc):
        if AggregationType.MIN in self.agg_on_funcs:
            v = self.agg_on_funcs[AggregationType.MIN](record.value)
            if AggregationType.MIN in accumulator.aggs:
                accumulator.aggs[AggregationType.MIN] = min(accumulator.aggs[AggregationType.MIN], v)
            else:
                accumulator.aggs[AggregationType.MIN] = v

        if AggregationType.MAX in self.agg_on_funcs:
            v = self.agg_on_funcs[AggregationType.MAX](record.value)
            if AggregationType.MAX in accumulator.aggs:
                accumulator.aggs[AggregationType.MAX] = max(accumulator.aggs[AggregationType.MAX], v)
            else:
                accumulator.aggs[AggregationType.MAX] = v

        if AggregationType.COUNT in accumulator.aggs:
            accumulator.aggs[AggregationType.COUNT] += Decimal(1)
        else:
            accumulator.aggs[AggregationType.COUNT] = Decimal(1)

        if AggregationType.SUM in self.agg_on_funcs or AggregationType.AVG in self.agg_on_funcs:
            if AggregationType.SUM in self.agg_on_funcs:
                agg_func = self.agg_on_funcs[AggregationType.SUM]
            else:
                agg_func = self.agg_on_funcs[AggregationType.AVG]

            v = agg_func(record.value)
            if AggregationType.SUM in accumulator.aggs:
                accumulator.aggs[AggregationType.SUM] += Decimal(v)
            else:
                accumulator.aggs[AggregationType.SUM] = Decimal(v)

            accumulator.aggs[AggregationType.AVG] = accumulator.aggs[AggregationType.SUM]/accumulator.aggs[AggregationType.COUNT]

    def get_result(self, accumulator: Any) -> Any:
        return accumulator.aggs

    def merge(self, acc1: Any, acc2: Any) -> Any:
        raise NotImplementedError()








