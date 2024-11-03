import enum
import logging
from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional

from ray.actor import ActorHandle
import ray

from volga.streaming.api.collector.collector import Collector, CollectionCollector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import Function, SourceContext, SourceFunction, MapFunction, \
    FlatMapFunction, FilterFunction, KeyFunction, ReduceFunction, SinkFunction, EmptyFunction, JoinFunction
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operators.timestamp_assigner import TimestampAssigner
from volga.streaming.common.utils import now_ts_ms
from volga.streaming.runtime.master.stats.stats_manager import WorkerLatencyStatsState, WorkerThroughputStatsState
from volga.streaming.runtime.sources.source_splits_manager import SourceSplitEnumerator, SourceSplit, SourceSplitType

logger = logging.getLogger(__name__)


class OperatorType(enum.Enum):
    SOURCE = 0  # Sources are where your program reads its input from
    ONE_INPUT = 1  # This operator has one data stream as it's input stream.
    TWO_INPUT = 2  # This operator has two data stream as it's input stream.


class Operator(ABC):
    # Abstract base class for all operators. An operator is used to run a Function

    @abstractmethod
    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        pass

    @abstractmethod
    def finish(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def operator_type(self) -> OperatorType:
        pass


class OneInputOperator(Operator, ABC):

    @abstractmethod
    def process_element(self, record: Record):
        pass

    def operator_type(self):
        return OperatorType.ONE_INPUT


class TwoInputOperator(Operator, ABC):

    @abstractmethod
    def process_element(self, left: Record, right: Record):
        pass

    def operator_type(self):
        return OperatorType.TWO_INPUT


class StreamOperator(Operator, ABC):

    def __init__(self, func: Optional[Function] = None):
        self.func = func # chained operators do not have this
        self.collectors = None
        self.runtime_context = None

        # set at jobGraph compilation
        self.id = None

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        self.collectors = collectors
        self.runtime_context = runtime_context
        self.func.open(runtime_context)

    def close(self):
        self.func.close()

    def finish(self):
        pass

    def collect(self, record: Record):
        for collector in self.collectors:
            collector.collect(record)


class ISourceOperator(StreamOperator, ABC):

    @abstractmethod
    def get_source_context(self) -> SourceContext:
        raise NotImplementedError

    @abstractmethod
    def fetch(self):
        raise NotImplementedError()

    def operator_type(self):
        return OperatorType.SOURCE


class SourceOperator(ISourceOperator):

    class SourceContextImpl(SourceContext):
        def __init__(
            self,
            collectors: List[Collector],
            runtime_context: RuntimeContext,
            timestamp_assigner: Optional[TimestampAssigner],
            num_records: Optional[int],
        ):
            self.collectors = collectors
            self.runtime_context = runtime_context
            self.timestamp_assigner = timestamp_assigner
            self.num_records = num_records
            self.num_fetched_records = 0
            self.finished = False
            self.current_split: Optional[SourceSplit] = None
            self.job_master: Optional[ActorHandle] = None
            self.throughput_stats = WorkerThroughputStatsState.create()

        def collect(self, value: Any):
            source_emit_ts = None
            # throttle source_emit_ts setting - it is used to calculate latency stats, we dont want it on every message
            if (self.num_fetched_records + 1)%100 == 0:
                source_emit_ts = now_ts_ms()

            for collector in self.collectors:
                record = Record(value=value, event_time=None, source_emit_ts=source_emit_ts)
                if self.timestamp_assigner is not None:
                    record = self.timestamp_assigner.assign_timestamp(record)
                collector.collect(record)
            self.num_fetched_records += 1
            self.throughput_stats.inc()

            # two ways notify reached bounds
            # 1. if source function implements num_records (bounded non-split source)
            if self.num_records == self.num_fetched_records:
                # set finished state
                self.finished = True

        def get_current_split(self) -> SourceSplit:
            if self.current_split is None:
                self.current_split = self.poll_next_split()

            # two ways notify reached bounds
            # 2. if source is a split-source and we have reached END_OF_INPUT
            if self.current_split.type == SourceSplitType.END_OF_INPUT:
                self.finished = True
            return self.current_split

        def poll_next_split(self) -> SourceSplit:
            split = ray.get(self.job_master.poll_next_source_split.remote(
                self.runtime_context.operator_id,
                self.runtime_context.task_id
            ))
            self.current_split = split

            # two ways notify reached bounds
            # 2. if source is a split-source and we have reached END_OF_INPUT
            if self.current_split.type == SourceSplitType.END_OF_INPUT:
                self.finished = True
            return self.current_split

        def set_master_handle(self, job_master: ActorHandle):
            self.job_master = job_master

    def __init__(self, func: SourceFunction):
        super().__init__(func)
        self.source_context: Optional[SourceOperator.SourceContextImpl] = None
        self.timestamp_assigner: Optional[TimestampAssigner] = None
        self.split_enumerator: Optional[SourceSplitEnumerator] = None

    def set_timestamp_assigner(self, timestamp_assigner: TimestampAssigner):
        self.timestamp_assigner = timestamp_assigner

    def set_split_enumerator(self, split_enumerator: SourceSplitEnumerator):
        self.split_enumerator = split_enumerator

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)
        assert isinstance(self.func, SourceFunction)
        self.func.init(
            runtime_context.parallelism, runtime_context.task_index
        )
        num_records = None
        try:
            num_records = self.func.num_records()
        except NotImplementedError:
            # unbounded source or split-based source
            pass
        self.source_context = SourceOperator.SourceContextImpl(
            collectors,
            runtime_context=runtime_context,
            timestamp_assigner=self.timestamp_assigner,
            num_records=num_records
        )

    def fetch(self):
        self.func.fetch(self.get_source_context())

    def get_source_context(self) -> SourceContext:
        return self.source_context


class MapOperator(StreamOperator, OneInputOperator):

    def __init__(self, map_func: MapFunction):
        assert isinstance(map_func, MapFunction)
        super().__init__(map_func)

    def process_element(self, record):
        self.collect(Record(value=self.func.map(record.value), event_time=record.event_time, source_emit_ts=record.source_emit_ts))


class FlatMapOperator(StreamOperator, OneInputOperator):

    def __init__(self, flat_map_func: FlatMapFunction):
        assert isinstance(flat_map_func, FlatMapFunction)
        super().__init__(flat_map_func)
        self.collection_collector = None

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)
        self.collection_collector = CollectionCollector(collectors)

    def process_element(self, record: Record):
        self.func.flat_map(record.value, self.collection_collector)


class FilterOperator(StreamOperator, OneInputOperator):

    def __init__(self, filter_func: FilterFunction):
        assert isinstance(filter_func, FilterFunction)
        super().__init__(filter_func)

    def process_element(self, record: Record):
        if self.func.filter(record.value):
            self.collect(record)


class KeyByOperator(StreamOperator, OneInputOperator):

    def __init__(self, key_func: KeyFunction):
        assert isinstance(key_func, KeyFunction)
        super().__init__(key_func)

    def process_element(self, record: Record):
        key = self.func.key_by(record.value)
        self.collect(KeyRecord(key, record.value, record.event_time, record.source_emit_ts))


class ReduceOperator(StreamOperator, OneInputOperator):

    def __init__(self, reduce_func: ReduceFunction):
        assert isinstance(reduce_func, ReduceFunction)
        super().__init__(reduce_func)
        self.reduce_state = {}

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)

    def process_element(self, record: KeyRecord):
        key = record.key
        value = record.value
        if key in self.reduce_state:
            old_value = self.reduce_state[key]
            new_value = self.func.reduce(old_value, value)
            self.reduce_state[key] = new_value
            self.collect(Record(value=new_value, event_time=record.event_time, source_emit_ts=record.source_emit_ts))
        else:
            self.reduce_state[key] = value
            self.collect(record)


class SinkOperator(StreamOperator, OneInputOperator):

    def __init__(self, sink_func: SinkFunction):
        assert isinstance(sink_func, SinkFunction)
        super().__init__(sink_func)
        self.latency_stats = None
        
    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)
        self.latency_stats = WorkerLatencyStatsState.create()

    def process_element(self, record: Record):
        if record.source_emit_ts is not None:
            ts = now_ts_ms()
            latency = ts - record.source_emit_ts
            self.latency_stats.observe(latency, ts)
        self.func.sink(record.value)


class UnionOperator(StreamOperator, OneInputOperator):

    def __init__(self):
        super().__init__(EmptyFunction())

    def process_element(self, record: Record):
        self.collect(record)


class JoinOperator(StreamOperator, TwoInputOperator):

    def __init__(self, join_func: Optional[JoinFunction] = None):
        super().__init__(join_func)
        self.left_records_dict: Dict[Any, List[Record]] = {}
        self.right_records_dict: Dict[Any, List[Record]] = {}

    def process_element(self, left: KeyRecord, right: KeyRecord):
        if left is not None:
            key = left.key
        else:
            key = right.key

        if key in self.left_records_dict:
            left_records = self.left_records_dict[key]
        else:
            left_records = []
            self.left_records_dict[key] = left_records

        if key in self.right_records_dict:
            right_records = self.right_records_dict[key]
        else:
            right_records = []
            self.right_records_dict[key] = right_records

        # use left stream event time by default
        # TODO we should infer event time from values when we schema is implemented
        # TODO should we let user decide?

        if left is not None:
            lv = left.value
            event_time = left.event_time
            source_emit_ts = left.source_emit_ts
            left_records.append(left)
            for right_r in right_records:
                self.collect(Record(value=self.func.join(lv, right_r.value), event_time=event_time, source_emit_ts=source_emit_ts))
        else:
            rv = right.value
            right_records.append(right)
            for left_r in left_records:
                event_time = left_r.event_time
                source_emit_ts = left_r.source_emit_ts
                self.collect(Record(value=self.func.join(left_r.value, rv), event_time=event_time, source_emit_ts=source_emit_ts))
