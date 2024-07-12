import enum
import logging
from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional

from volga.streaming.api.collector.collector import Collector, CollectionCollector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import Function, SourceContext, SourceFunction, MapFunction, \
    FlatMapFunction, FilterFunction, KeyFunction, ReduceFunction, SinkFunction, EmptyFunction, JoinFunction
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operator.timestamp_assigner import TimestampAssigner

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

    def get_next_operators(self) -> List['Operator']:
        raise

    def add_next_operator(self, operator: 'Operator'):
        raise


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

    def __init__(self, func: Function):
        self.func = func
        self.collectors = None
        self.runtime_context = None

        # set at jobGraph compilation
        self.id = None

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        self.collectors = collectors
        self.runtime_context = runtime_context
        self.func.open(runtime_context)

    def finish(self):
        pass

    def close(self):
        self.func.close()

    def collect(self, record: Record):
        for collector in self.collectors:
            collector.collect(record)


class SourceOperator(StreamOperator):

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

        def collect(self, value: Any):
            for collector in self.collectors:
                record = Record(value)
                if self.timestamp_assigner is not None:
                    record = self.timestamp_assigner.assign_timestamp(record)
                collector.collect(record)
            self.num_fetched_records += 1

            # notify reached bounds
            if self.num_records == self.num_fetched_records:
                # set finished state
                self.finished = True

    def __init__(self, func: SourceFunction):
        assert isinstance(func, SourceFunction)
        super().__init__(func)
        self.source_context: Optional[SourceOperator.SourceContextImpl] = None
        self.timestamp_assigner: Optional[TimestampAssigner] = None

    def set_timestamp_assigner(self, timestamp_assigner: TimestampAssigner):
        self.timestamp_assigner = timestamp_assigner

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
            # unbounded source
            pass
        self.source_context = SourceOperator.SourceContextImpl(
            collectors,
            runtime_context=runtime_context,
            timestamp_assigner=self.timestamp_assigner,
            num_records=num_records
        )

    def fetch(self):
        self.func.fetch(self.source_context)

    def operator_type(self):
        return OperatorType.SOURCE


class MapOperator(StreamOperator, OneInputOperator):

    def __init__(self, map_func: MapFunction):
        assert isinstance(map_func, MapFunction)
        super().__init__(map_func)

    def process_element(self, record):
        self.collect(Record(value=self.func.map(record.value), event_time=record.event_time))


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
        self.collect(KeyRecord(key, record.value, record.event_time))


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
            self.collect(Record(value=new_value, event_time=record.event_time))
        else:
            self.reduce_state[key] = value
            self.collect(record)


class SinkOperator(StreamOperator, OneInputOperator):

    def __init__(self, sink_func: SinkFunction):
        assert isinstance(sink_func, SinkFunction)
        super().__init__(sink_func)

    def process_element(self, record: Record):
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
        self.i = 0

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
            left_records.append(left)
            for right_r in right_records:
                self.collect(Record(value=self.func.join(lv, right_r.value), event_time=event_time))
        else:
            rv = right.value
            right_records.append(right)
            for left_r in left_records:
                event_time = left_r.event_time
                self.collect(Record(value=self.func.join(left_r.value, rv), event_time=event_time))
