from abc import ABC
from collections import deque
from typing import List, Optional, Callable, Deque

from pydantic import BaseModel

from volga.common.time_utils import duration_to_ms, Duration
from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.aggregate_function import AggregationType, AllAggregateFunction
from volga.streaming.api.function.function import EmptyFunction
from volga.streaming.api.function.window_function import WindowFunction, AllAggregateApplyWindowFunction
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operator.operator import StreamOperator, OneInputOperator


class Window(BaseModel):
    records: Deque[Record]
    length_ms: int
    window_func: WindowFunction
    name: Optional[str] = None


class SlidingWindowConfig(BaseModel):
    duration: Duration
    agg_type: AggregationType
    agg_on: Callable
    name: Optional[str] = None


# tracks multiple sliding windows per key, triggers on each event
class MultiWindowOperator(StreamOperator, OneInputOperator):

    def __init__(self, configs: List[SlidingWindowConfig]):
        super().__init__(EmptyFunction())
        self.configs = configs
        self.windows_per_key = {}

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)

    def process_element(self, record: Record):
        assert isinstance(record, KeyRecord)
        key = record.key
        if key in self.windows_per_key:
            windows = self.windows_per_key[key]
        else:
            windows = self._create_windows()
            self.windows_per_key[key] = key

        aggs_per_window = {}
        for w in windows:
            w.records.append(record)

            # we assume events arrive in order
            # remove late events
            while w.records[-1].event_time - w.records[0].event_time > w.length_ms:
                w.records.popleft()
            if w.name in aggs_per_window:
                raise RuntimeError(f'Duplicate window names: {w.name}')
            aggs_per_window[w.name] = w.window_func.apply(w.records)

        self.collect(Record(value=aggs_per_window, event_time=record.event_time))

    def _create_windows(self) -> List[Window]:
        res = []
        for conf in self.configs:
            if conf.name is None:
                name = f'{conf.agg_type}_{conf.duration}'
            else:
                name = conf.name
            agg_func = AllAggregateFunction({conf.agg_type: conf.agg_on})
            window_func = AllAggregateApplyWindowFunction(agg_func)
            res.append(Window(
                records=deque(),
                length_ms=duration_to_ms(conf.duration),
                window_func=window_func,
                name=name
            ))
        return res

