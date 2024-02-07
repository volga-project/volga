from typing import List, Callable, Union

from volga.streaming.api.function.function import SimpleMapFunction, SimpleFlatMapFunction, \
    SimpleFilterFunction, SimpleKeyFunction, SimpleJoinFunction, SimpleReduceFunction, SimpleSinkFunction, Function
from volga.streaming.api.operator.operator import MapOperator, FlatMapOperator, FilterOperator, \
    ReduceOperator, StreamOperator, JoinOperator, KeyByOperator, SinkOperator, MultiWindowOperator
from volga.streaming.api.partition.partition import KeyPartition
from volga.streaming.api.stream.stream import Stream
from volga.streaming.api.stream.stream_sink import StreamSink
from volga.streaming.api.stream.window import WindowAssigner, WindowTrigger

FunctionOrCallable = Union[Function, Callable]


class DataStream(Stream):

    def map(self, map_func: FunctionOrCallable) -> 'DataStream':
        if isinstance(map_func, Callable):
            map_func = SimpleMapFunction(map_func)
        return DataStream(
            input_stream=self,
            stream_operator=MapOperator(map_func),
        )

    def flat_map(self, flat_map_func: FunctionOrCallable) -> 'DataStream':
        if isinstance(flat_map_func, Callable):
            flat_map_func = SimpleFlatMapFunction(flat_map_func)
        return DataStream(
            input_stream=self,
            stream_operator=FlatMapOperator(flat_map_func),
        )

    def filter(self, filter_func: FunctionOrCallable) -> 'DataStream':
        if isinstance(filter_func, Callable):
            filter_func = SimpleFilterFunction(filter_func)
        return DataStream(
            input_stream=self,
            stream_operator=FilterOperator(filter_func),
        )

    def join(self, other: 'DataStream') -> 'JoinStream':
        return JoinStream(
            left_stream=self,
            right_stream=other
        )

    def key_by(self, key_by_func: FunctionOrCallable) -> 'KeyDataStream':
        if isinstance(key_by_func, Callable):
            key_by_func = SimpleKeyFunction(key_by_func)
        return KeyDataStream(
            input_stream=self,
            stream_operator=KeyByOperator(key_by_func)
        )

    def sink(self, sink_func: FunctionOrCallable) -> 'StreamSink':
        if isinstance(sink_func, Callable):
            sink_func = SimpleSinkFunction(sink_func)
        return StreamSink(
            input_stream=self,
            sink_operator=SinkOperator(sink_func)
        )

    # TODO union, broadcast, partition_by, process
    # def union(self, streams: List[Stream]) -> 'DataStream':


# join
class JoinStream(DataStream):

    def __init__(
        self,
        left_stream: DataStream,
        right_stream: DataStream,
    ):
        super().__init__(input_stream=left_stream, stream_operator=JoinOperator())
        self.right_stream = right_stream

    def where_key(self, key_by_func: FunctionOrCallable) -> 'JoinWhere':
        if isinstance(key_by_func, Callable):
            key_by_func = SimpleKeyFunction(key_by_func)
        self.stream_operator.left_key_by_function = key_by_func
        return JoinWhere(join_stream=self)


class JoinEqual:

    def __init__(
        self,
        join_stream: 'JoinStream'
    ):
        self.join_stream = join_stream

    def with_func(self, join_func: FunctionOrCallable) -> DataStream:
        if isinstance(join_func, Callable):
            join_func = SimpleJoinFunction(join_func)
        self.join_stream.stream_operator.func = join_func
        return self.join_stream


class JoinWhere:

    def __init__(
        self,
        join_stream: 'JoinStream'
    ):
        self.join_stream = join_stream

    def equal_to(self, right_key_by_func: FunctionOrCallable) -> JoinEqual:
        if isinstance(right_key_by_func, Callable):
            right_key_by_func = SimpleKeyFunction(right_key_by_func)
        self.join_stream.stream_operator.right_key_by_function = right_key_by_func
        return JoinEqual(self.join_stream)


# key_by
class KeyDataStream(DataStream):

    def __init__(
        self,
        input_stream: Stream,
        stream_operator: StreamOperator
    ):
        super().__init__(input_stream=input_stream, stream_operator=stream_operator, partition=KeyPartition())

    def reduce(self, reduce_func: FunctionOrCallable) -> DataStream:
        if isinstance(reduce_func, Callable):
            reduce_func = SimpleReduceFunction(reduce_func)
        return DataStream(input_stream=self, stream_operator=ReduceOperator(reduce_func))

    def aggregate(self, aggregate_func: FunctionOrCallable) -> DataStream:
        # TODO implement keyed aggregation
        raise NotImplementedError()

    def multi_window_agg(self):
        return DataStream(input_stream=self, stream_operator=MultiWindowOperator())


# union
class UnionStream(DataStream):

    def __init__(self, streams: List[Stream]):
        # TODO call super
        self.union_streams = streams
