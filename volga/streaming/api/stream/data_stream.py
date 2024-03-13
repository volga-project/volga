from typing import List, Callable, Union, Optional

from volga.streaming.api.function.function import SimpleMapFunction, SimpleFlatMapFunction, \
    SimpleFilterFunction, SimpleKeyFunction, SimpleJoinFunction, SimpleReduceFunction, SimpleSinkFunction, Function
from volga.streaming.api.operator.operator import MapOperator, FlatMapOperator, FilterOperator, \
    ReduceOperator, StreamOperator, JoinOperator, KeyByOperator, SinkOperator
from volga.streaming.api.operator.window_operator import MultiWindowOperator, SlidingWindowConfig, OutputWindowFunc
from volga.streaming.api.partition.partition import KeyPartition
from volga.streaming.api.stream.stream import Stream
from volga.streaming.api.stream.stream_sink import StreamSink

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

    # TODO union, broadcast, process
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

    def with_func(self, join_func: FunctionOrCallable) -> DataStream:
        if isinstance(join_func, Callable):
            join_func = SimpleJoinFunction(join_func)
        self.stream_operator.func = join_func
        return self


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

    def join(self, other: 'KeyDataStream') -> 'JoinStream':
        return JoinStream(
            left_stream=self,
            right_stream=other
        )

    def aggregate(self, aggregate_func: FunctionOrCallable) -> DataStream:
        # TODO implement keyed aggregation
        raise NotImplementedError()

    def multi_window_agg(self, configs: List[SlidingWindowConfig], output_func: Optional[OutputWindowFunc] = None) -> DataStream:
        return DataStream(input_stream=self, stream_operator=MultiWindowOperator(configs, output_func))


# union
class UnionStream(DataStream):

    def __init__(self, streams: List[Stream]):
        # TODO call super
        self.union_streams = streams
