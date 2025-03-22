import inspect
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Any, Dict, List, Union, Callable, Tuple

from ray.actor import ActorHandle

from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.common.utils import collection_chunk_at_index
from volga.streaming.runtime.sources.source_splits_manager import SourceSplit


class Function(ABC):

    def open(self, runtime_context: RuntimeContext):
        pass

    def close(self):
        pass


class EmptyFunction(Function):
    pass


class SourceContext(ABC):
    # Interface that source functions use to emit elements, and possibly watermarks

    @abstractmethod
    def collect(self, element: Any):
        # Emits one element from the source, without attaching a timestamp
        raise NotImplementedError()

    @abstractmethod
    def set_master_handle(self, job_master: ActorHandle):
        raise NotImplementedError()

    @abstractmethod
    def get_current_split(self) -> SourceSplit:
        # Returns current split
        raise NotImplementedError()

    @abstractmethod
    def poll_next_split(self) -> SourceSplit:
        # Polls next split, marks current as done
        # TODO we should asyncify this
        raise NotImplementedError()


class SourceFunction(Function):
    # Interface of Source functions

    @abstractmethod
    def init(self, parallel: int, index: int):
        pass

    @abstractmethod
    def fetch(self, ctx: SourceContext):
        pass

    # TODO these should be retired in favor of a proper shutdown mechanism (via terminal_message)
    def num_records(self) -> int:
        # if source is bounded, this should return expected number of records
        raise NotImplementedError()

    def get_num_sent(self) -> Any:
        raise NotImplementedError()


class MapFunction(Function):

    @abstractmethod
    def map(self, value: Any):
        pass


class FlatMapFunction(Function):

    @abstractmethod
    def flat_map(self, value) -> List:
        # Takes an element from the input data set and transforms it into zero, one, or more elements.
        pass


class FilterFunction(Function):

    @abstractmethod
    def filter(self, value):
        pass


class KeyFunction(Function):

    @abstractmethod
    def key_by(self, value):
        # extracts key from the object
        pass


class ReduceFunction(Function):

    @abstractmethod
    def reduce(self, old_value, new_value):
        # combines two values into one value of the same type
        pass


class SinkFunction(Function):

    @abstractmethod
    def sink(self, value):
        # Writes the given value to the sink. This function is called for every record
        pass


class JoinFunction(Function):

    @abstractmethod
    def join(self, left, right):
        pass


class CollectionSourceFunction(SourceFunction):
    def __init__(self, all_values):
        self.all_values = all_values
        self.values = None
        self.num_values = None
        self.num_sent = 0


    def init(self, parallel, index):
        self.values = collection_chunk_at_index(self.all_values, parallel, index)
        self.num_values = len(self.values)

    def fetch(self, ctx: SourceContext):
        for v in self.values:
            ctx.collect(v)
        self.num_sent += len(self.values)
        self.values = []

    def num_records(self) -> int:
        return self.num_values
    
    def get_num_sent(self) -> Any:
        return self.num_sent


class LocalFileSourceFunction(SourceFunction):
    def __init__(self, filename):
        self.filename = filename
        self.done = False

    def init(self, parallel, index):
        if parallel > 1:
            raise RuntimeError('Does not support parallelism > 1')

    def fetch(self, ctx: SourceContext):
        if self.done:
            return
        with open(self.filename, "r") as f:
            line = f.readline()
            while line != "":
                ctx.collect(line[:-1])
                line = f.readline()
            self.done = True


class PeriodicCollectionSourceFunction(SourceFunction):
    def __init__(self, all_values, period_s):
        self.period_s = period_s
        self.all_values = all_values
        self.values = None
        self.num_values = None

    def init(self, parallel, index):
        self.values = collection_chunk_at_index(self.all_values, parallel, index)
        self.num_values = len(self.values)

    def fetch(self, ctx: SourceContext):
        for i in range(len(self.values)):
            ctx.collect(self.values[i])
            if i < len(self.values) - 1:
                time.sleep(self.period_s)
        self.values = []

    def num_records(self) -> int:
        return len(self.values)


class SimpleMapFunction(MapFunction):
    def __init__(self, func):
        self.func = func

    def map(self, value):
        return self.func(value)


class SimpleFlatMapFunction(FlatMapFunction):

    def __init__(self, func):
        self.func = func

    def flat_map(self, value) -> List:
        return self.func(value)


class SimpleFilterFunction(FilterFunction):
    def __init__(self, func):
        self.func = func

    def filter(self, value):
        return self.func(value)


class SimpleKeyFunction(KeyFunction):
    def __init__(self, func):
        self.func = func

    def key_by(self, value):
        return self.func(value)


class SimpleReduceFunction(ReduceFunction):
    def __init__(self, func):
        self.func = func

    def reduce(self, old_value, new_value):
        return self.func(old_value, new_value)


class SimpleJoinFunction(JoinFunction):
    def __init__(self, func):
        self.func = func

    def join(self, left, right):
        return self.func(left, right)


class SimpleSinkFunction(SinkFunction):
    def __init__(self, func):
        self.func = func

    def sink(self, value):
        return self.func(value)


class SinkToCacheFunctionBase(SinkFunction, ABC):

    DUMPER_PERIOD_S = 1

    def __init__(self, cache_actor: ActorHandle):
        self.cache_actor = cache_actor
        self.buffer = self.init_buffer()
        self.dumper_thread = None
        self.running = False

    @abstractmethod
    def sink(self, value):
        raise NotImplementedError()

    @abstractmethod
    def init_buffer(self) -> Union[List, Dict]:
        raise NotImplementedError()

    @abstractmethod
    def dump_buffer_if_needed(self):
        raise NotImplementedError()

    def _dump_buffer_loop(self):
        while self.running:
            self.dump_buffer_if_needed()
            time.sleep(self.DUMPER_PERIOD_S)

    def open(self, runtime_context: RuntimeContext):
        self.running = True
        self.dumper_thread = Thread(target=self._dump_buffer_loop)
        self.dumper_thread.start()

    def close(self):
        self.running = False
        self.dump_buffer_if_needed()
        if self.dumper_thread is not None:
            self.dumper_thread.join(timeout=5)


class SinkToCacheDictFunction(SinkToCacheFunctionBase):

    def __init__(self, cache_actor: ActorHandle, key_value_extractor: Callable[[Any], Tuple]):
        super().__init__(cache_actor)
        self._key_value_extractor = key_value_extractor

    def sink(self, value):
        key, value = self._key_value_extractor(value)
        self.buffer[key] = value

    def init_buffer(self) -> Dict:
        return {}

    def dump_buffer_if_needed(self):
        if len(self.buffer) == 0:
            return
        self.cache_actor.extend_dict.remote(self.buffer)
        self.buffer = {}


class SinkToCacheListFunction(SinkToCacheFunctionBase):

    def sink(self, value):
        self.buffer.append(value)

    def init_buffer(self) -> List:
        return []

    def dump_buffer_if_needed(self):
        if len(self.buffer) == 0:
            return
        self.cache_actor.extend_list.remote(self.buffer)
        self.buffer = []
