from abc import ABC, abstractmethod
from volga.streaming.api.function.function import SinkFunction
from volga.api.schema import Schema
from volga.storage.common.in_memory_actor import InMemoryCacheActor, CACHE_ACTOR_NAME
import time
from threading import Thread
from volga.common.time_utils import datetime_str_to_ts
from volga.streaming.api.context.runtime_context import RuntimeContext
import ray


class BatchSinkToCacheActorFunction(SinkFunction):

    DUMPER_PERIOD_S = 1

    def __init__(self, feature_name: str, output_schema: Schema):
        self.cache_actor = None
        self.feature_name = feature_name
        self.output_schema = output_schema
        self.batch = []
        self.dumper_thread = None
        self.running = False
        
    def sink(self, value):
        key_fields = list(self.output_schema.keys.keys())
        keys_dict = {k: value[k] for k in key_fields}
        timestamp_field = self.output_schema.timestamp
        ts = datetime_str_to_ts(value[timestamp_field].isoformat())
        self.batch.append((keys_dict, ts, value))

    def _dump_batch_if_needed(self):
        if len(self.batch) == 0:
            return
        self.cache_actor.put_records.remote(self.feature_name, self.batch)
        self.batch = []

    def _dump_batch_loop(self):
        while self.running:
            self._dump_batch_if_needed()
            time.sleep(self.DUMPER_PERIOD_S)

    def open(self, runtime_context: RuntimeContext):
        self.cache_actor = InMemoryCacheActor.options(name=CACHE_ACTOR_NAME, get_if_exists=True).remote()
        ray.get(self.cache_actor.ready.remote())
        self.running = True
        self.dumper_thread = Thread(target=self._dump_batch_loop)
        self.dumper_thread.start()

    def close(self):
        self.running = False
        self._dump_batch_if_needed()
        if self.dumper_thread is not None:
            self.dumper_thread.join(timeout=5)


class StreamSinkToCacheActorFunction(SinkFunction):

    def __init__(self, feature_name: str, output_schema: Schema):
        self.cache_actor = None
        self.feature_name = feature_name
        self.output_schema = output_schema

    def open(self, runtime_context: RuntimeContext):
        self.cache_actor = InMemoryCacheActor.options(name=CACHE_ACTOR_NAME, get_if_exists=True).remote()
        ray.get(self.cache_actor.ready.remote())

    def sink(self, value):
        # TODO add limit on ongoing requests
        key_fields = list(self.output_schema.keys.keys())
        keys_dict = {k: value[k] for k in key_fields}
        timestamp_field = self.output_schema.timestamp
        ts = datetime_str_to_ts(value[timestamp_field].isoformat())
        self.cache_actor.put_records.remote(self.feature_name, [(keys_dict, ts, value)])

class PipelineDataConnector(ABC):

    @abstractmethod
    def get_sink_function(self, feature_name: str, output_schema: Schema) -> SinkFunction:
        pass

class InMemoryActorPipelineDataConnector(PipelineDataConnector):
    def __init__(self, batch: bool = False):
        self.batch = batch

    def get_sink_function(self, feature_name: str, output_schema: Schema) -> SinkFunction:
        if self.batch:
            return BatchSinkToCacheActorFunction(feature_name, output_schema)
        else:
            return StreamSinkToCacheActorFunction(feature_name, output_schema)
