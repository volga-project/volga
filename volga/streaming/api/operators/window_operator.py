import bisect
from collections import deque
from dataclasses import dataclass
from typing import List, Optional, Callable, Deque, Dict

from pydantic import BaseModel
from decimal import Decimal

from volga.common.time_utils import Duration, duration_to_s
from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.aggregate_function import AggregationType, AllAggregateFunction
from volga.streaming.api.function.function import EmptyFunction
from volga.streaming.api.function.window_function import WindowFunction, AllAggregateApplyWindowFunction
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operators.operators import StreamOperator, OneInputOperator


@dataclass
class Window:
    records: List
    length_s: Decimal
    window_func: WindowFunction
    name: str
    agg_type: AggregationType


class SlidingWindowConfig(BaseModel):
    duration: Duration
    agg_type: AggregationType
    agg_on_func: Optional[Callable]
    name: Optional[str] = None


AggregationsPerWindow = Dict[str, Decimal]  # window name agg value
OutputWindowFunc = Callable[[AggregationsPerWindow, Record], Record] # forms output record


# tracks multiple sliding windows per key, triggers on each event
class MultiWindowOperator(StreamOperator, OneInputOperator):

    def __init__(
        self,
        configs: List[SlidingWindowConfig],
        output_func: Optional[OutputWindowFunc] = None
    ):
        super().__init__(EmptyFunction())
        self.configs = configs
        self.windows_per_key = {}
        self.output_func = output_func

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)

    def process_element(self, record: Record):
        assert isinstance(record, KeyRecord)
        key = record.key
        if key in self.windows_per_key:
            windows = self.windows_per_key[key]
        else:
            windows = self._create_windows()
            self.windows_per_key[key] = windows

        aggs_per_window: AggregationsPerWindow = {}
        for w in windows:

            # this is O(logn) search + O(n) insert
            bisect.insort_right(w.records, record, key=(lambda r: r.event_time))

            # remove out of time events
            while w.records[-1].event_time - w.records[0].event_time > w.length_s:
                w.records.pop(0)
            if w.name in aggs_per_window:
                raise RuntimeError(f'Duplicate window names: {w.name}')
            accum = w.window_func.apply(w.records)
            assert isinstance(accum, AllAggregateFunction._Acc)
            aggs_per_window[w.name] = accum.aggs[w.agg_type]

        if self.output_func is None:
            output_record = Record(value=aggs_per_window, event_time=record.event_time, source_emit_ts=record.source_emit_ts)
        else:
            output_record = self.output_func(aggs_per_window, record)

        output_record.set_stream_name(record.stream_name)
        self.collect(output_record)

    def _create_windows(self) -> List[Window]:
        res = []
        for conf in self.configs:
            if conf.name is None:
                name = f'{conf.agg_type}_{conf.duration}'
            else:
                name = conf.name
            agg_func = AllAggregateFunction(conf.agg_type, conf.agg_on_func)
            window_func = AllAggregateApplyWindowFunction(agg_func)
            res.append(Window(
                records=[],
                length_s=duration_to_s(conf.duration),
                window_func=window_func,
                name=name,
                agg_type=conf.agg_type
            ))
        return res
