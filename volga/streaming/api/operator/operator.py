import enum
import logging
from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional

from volga.streaming.api.collector.collector import Collector, CollectionCollector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import Function, SourceContext, SourceFunction, MapFunction, \
    FlatMapFunction, FilterFunction, KeyFunction, ReduceFunction, SinkFunction, EmptyFunction, JoinFunction, \
    AggregateFunction
from volga.streaming.api.function.window_agg import MultiWindowAggregateFunction
from volga.streaming.api.message.message import Record, KeyRecord

logger = logging.getLogger(__name__)


class OperatorType(enum.Enum):
    SOURCE = 0  # Sources are where your program reads its input from
    ONE_INPUT = 1  # This operator has one data stream as it's input stream.
    TWO_INPUT = 2  # This operator has two data stream as it's input stream.


class Operator(ABC):
    """
    Abstract base class for all operators.
    An operator is used to run a :class:`function.Function`.
    """

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
    """
    Basic interface for stream operators. Implementers would implement one of
    :class:`OneInputOperator` or :class:`TwoInputOperator` to create
    operators that process elements.
    """

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
        def __init__(self, collectors: List[Collector]):
            self.collectors = collectors

        def collect(self, value: Any):
            for collector in self.collectors:
                collector.collect(Record(value))

    def __init__(self, func: SourceFunction):
        assert isinstance(func, SourceFunction)
        super().__init__(func)
        self.source_context = None

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)
        self.source_context = SourceOperator.SourceContextImpl(collectors)
        self.func.init(
            runtime_context.parallelism, runtime_context.task_index
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
        self.collect(Record(self.func.map(record.value)))


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
        self.collect(KeyRecord(key, record.value))


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
            self.collect(Record(new_value))
        else:
            self.reduce_state[key] = value
            self.collect(record)


class WindowOperator(StreamOperator, OneInputOperator):

    def __init__(self, agg_func: MultiWindowAggregateFunction):
        assert isinstance(agg_func, MultiWindowAggregateFunction)
        super().__init__(agg_func)
        self.agg_func = agg_func

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        super().open(collectors, runtime_context)

    def process_element(self, record: Record):
        pass


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
        self.left_records_dict: Dict[Any, List[Any]] = {}
        self.right_records_dict: Dict[Any, List[Any]] = {}
        self.left_key_by_function = None
        self.right_key_by_function = None

    # TODO test
    def process_element(self, left: Record, right: Record):
        if left is not None:
            key = self.left_key_by_function.key_by(left.value)
        else:
            key = self.right_key_by_function.key_by(right.value)

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

        if left is not None:
            lv = left.value
            left_records.append(lv)
            self.collect(Record(self.func.join(lv, None)))
            for rv in right_records:
                self.collect(Record(self.func.join(lv, rv)))
        else:
            rv = right.value
            right_records.append(rv)
            self.collect(Record(self.func.join(None, rv)))
            for lv in left_records:
                self.collect(Record(self.func.join(lv, rv)))
