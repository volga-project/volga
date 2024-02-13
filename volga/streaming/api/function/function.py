import inspect
import sys
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Any

from ray import cloudpickle
from ray.actor import ActorHandle
from ray.util.client import ray

from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.message.message import Record


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
        pass


class SourceFunction(Function):
    # Interface of Source functions

    @abstractmethod
    def init(self, parallel: int, index: int):
        """
        Args:
            parallel: parallelism of source function
            index: task index of this function and goes up from 0 to
             parallel-1.
        """
        pass

    @abstractmethod
    def fetch(self, ctx: SourceContext):
        """Starts the source. Implementations can use the
        :class:`SourceContext` to emit elements.
        """
        pass

    def num_records(self) -> int:
        # if source is bounded, this should return expected number of records
        raise NotImplementedError()


class MapFunction(Function):

    @abstractmethod
    def map(self, value: Any):
        pass


class FlatMapFunction(Function):

    @abstractmethod
    def flat_map(self, value, collector):
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
    def __init__(self, values):
        self.values = values
        self.num_values = len(values)

    def init(self, parallel, index):
        pass

    def fetch(self, ctx: SourceContext):
        for v in self.values:
            ctx.collect(v)
        self.values = []

    def num_records(self) -> int:
        return self.num_values


class LocalFileSourceFunction(SourceFunction):
    def __init__(self, filename):
        self.filename = filename
        self.done = False

    def init(self, parallel, index):
        pass

    def fetch(self, ctx: SourceContext):
        if self.done:
            return
        with open(self.filename, "r") as f:
            line = f.readline()
            while line != "":
                ctx.collect(line[:-1])
                line = f.readline()
            self.done = True


class TimedCollectionSourceFunction(SourceFunction):
    def __init__(self, values, time_period_s):
        self.values = values
        self.time_period_s = time_period_s

    def init(self, parallel, index):
        pass

    def fetch(self, ctx: SourceContext):
        for i in range(len(self.values)):
            ctx.collect(self.values[i])
            if i < len(self.values) - 1:
                time.sleep(self.time_period_s)
        self.values = []


class SimpleMapFunction(MapFunction):
    def __init__(self, func):
        self.func = func

    def map(self, value):
        return self.func(value)


class SimpleFlatMapFunction(FlatMapFunction):
    """
    Wrap a python function as :class:`FlatMapFunction`

    >>> assert SimpleFlatMapFunction(lambda x: x.split())
    >>> def flat_func(x, collector):
    ...     for item in x.split():
    ...         collector.collect(item)
    >>> assert SimpleFlatMapFunction(flat_func)
    """

    def __init__(self, func):
        """
        Args:
            func: a python function which takes an element from input augment
            and transforms it into zero, one, or more elements.
            Or takes an element from input augment, and used provided collector
            to collect zero, one, or more elements.
        """
        self.func = func
        self.process_func = None
        sig = inspect.signature(func)
        assert (
            len(sig.parameters) <= 2
        ), "func should receive value [, collector] as arguments"
        if len(sig.parameters) == 2:

            def process(value, collector):
                func(value, collector)

            self.process_func = process
        else:

            def process(value, collector):
                for elem in func(value):
                    collector.collect(elem)

            self.process_func = process

    def flat_map(self, value, collector):
        self.process_func(value, collector)


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


class SinkToCacheFunction(SinkFunction):

    DUMPER_PERIOD_S = 1

    def __init__(self, cache_actor: ActorHandle):
        self.cache_actor = cache_actor
        self.buffer = []
        self.dumper_thread = None
        self.running = False

    def sink(self, value):
        self.buffer.append(value)

    def _dump_buffer_if_needed(self):
        if len(self.buffer) == 0:
            return
        self.cache_actor.extend_values.remote(self.buffer)
        self.buffer = []

    def _dump_buffer_loop(self):
        while self.running:
            self._dump_buffer_if_needed()
            time.sleep(self.DUMPER_PERIOD_S)

    def open(self, runtime_context: RuntimeContext):
        self.running = True
        self.dumper_thread = Thread(target=self._dump_buffer_loop)
        self.dumper_thread.start()

    def close(self):
        self.running = False
        self._dump_buffer_if_needed()
        if self.dumper_thread is not None:
            self.dumper_thread.join(timeout=5)


def serialize(func: Function):
    """Serialize a streaming :class:`Function`"""
    return cloudpickle.dumps(func)


def deserialize(func_bytes):
    """Deserialize a binary function serialized by `serialize` method."""
    return cloudpickle.loads(func_bytes)


def _get_simple_function_class(function_interface):
    """Get the wrapper function for the given `function_interface`."""
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, function_interface):
            if obj is not function_interface and obj.__name__.startswith("Simple"):
                return obj
    raise Exception("SimpleFunction for {} doesn't exist".format(function_interface))
