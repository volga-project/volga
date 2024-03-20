import bisect
import heapq
from threading import Thread
from typing import Dict, Any, Optional, List, Tuple

import ray
from decimal import Decimal
from ray.actor import ActorHandle
import time

from volga.common.time_utils import datetime_str_to_ts
from volga.data.api.dataset.schema import DataSetSchema
from volga.storage.cold.cold import ColdStorage
from volga.storage.common.key_index import compose_main_key, KeyIndex
from volga.storage.hot.hot import HotStorage
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SinkFunction


class SimpleInMemoryActorStorage(ColdStorage, HotStorage):

    CACHE_ACTOR_NAME = 'cache_actor'

    def __init__(self, dataset_name: str, output_schema: DataSetSchema):
        self.dataset_name = dataset_name
        self.output_schema = output_schema
        self.cache_actor = SimpleInMemoryCacheActor.options(name=self.CACHE_ACTOR_NAME, get_if_exists=True).remote()

    def gen_sink_function(self) -> SinkFunction:
        return BulkSinkToCacheActorFunction(
            cache_actor=self.cache_actor,
            dataset_name=self.dataset_name,
            output_schema=self.output_schema
        )

    def get_data(self, dataset_name: str, keys: Dict[str, Any], start_ts: Optional[Decimal], end_ts: Optional[Decimal]) -> List[Any]:
        return ray.get(self.cache_actor.get_values.remote(dataset_name, keys, start_ts, end_ts))

    def get_latest_data(self, dataset_name: str, keys: Dict[str, Any]) -> Any:
        raise NotImplementedError()


@ray.remote(num_cpus=0.01) # TODO set memory request
class SimpleInMemoryCacheActor:
    def __init__(self):
        self.per_dataset_per_key: Dict[str, Dict[str, List[Tuple[Decimal, Any]]]] = {}
        self.key_index_per_dataset: Dict[str, KeyIndex] = {}

    def put_records(self, dataset_name: str, records: List[Tuple[Dict[str, Any], Decimal, Any]]):
        if dataset_name in self.per_dataset_per_key:
            per_key = self.per_dataset_per_key[dataset_name]
            key_index = self.key_index_per_dataset[dataset_name]
        else:
            per_key = {}
            key_index = KeyIndex()
            self.per_dataset_per_key[dataset_name] = per_key
            self.key_index_per_dataset[dataset_name] = key_index

        for record in records:
            keys_dict = record[0]
            ts = record[1]
            value = record[2]
            key = compose_main_key(keys_dict)
            key_index.put(keys_dict)

            if key in per_key:
                ts_vals = per_key[key]
            else:
                ts_vals = []
                per_key[key] = ts_vals

            bisect.insort_right(ts_vals, (ts, value))

    def get_values(self, dataset_name: str, keys_dict: Dict[str, Any], start: Optional[Decimal], end: Optional[Decimal]) -> List:
        if dataset_name not in self.per_dataset_per_key:
            # raise RuntimeError(f'No dataset {dataset_name}')
            return []

        main_key = compose_main_key(keys_dict)
        possible_keys = self.key_index.get(keys_dict)
        possible_keys.append(main_key)
        res = []

        for key in possible_keys:
            timestamped_values = self.per_dataset_per_key[dataset_name][key]
            # remove timestamp keys
            vals = list(map(lambda v: v[1], timestamped_values))

            # range query
            first = bisect.bisect_left(vals, start) if start is not None else 0
            last = bisect.bisect_right(vals, end) if end is not None else vals[-1]

            v = vals[first:last]
            res = list(heapq.merge(res, v))

        return res

    def get_latest(self, dataset_name: str, keys_dict: Dict[str, Any]) -> Optional[Any]:
        vals = self.get_values(dataset_name=dataset_name, keys_dict=keys_dict, start=None, end=None)
        if len(vals) == 0:
            return None
        return vals[-1]


class BulkSinkToCacheActorFunction(SinkFunction):

    DUMPER_PERIOD_S = 1

    def __init__(self, cache_actor: ActorHandle, dataset_name: str, output_schema: DataSetSchema):
        self.cache_actor = cache_actor
        self.dataset_name = dataset_name
        self.output_schema = output_schema
        self.buffer = []
        self.dumper_thread = None
        self.running = False

    def sink(self, value):
        # TODO all of this should be a part of Record object and not sourced from outside
        key_fields = list(self.output_schema.keys.values())
        keys_dict = {k: value[k] for k in key_fields}
        timestamp_field = self.output_schema.timestamp
        ts = datetime_str_to_ts(value[timestamp_field])
        self.buffer.append((keys_dict, ts, value))

    def _dump_buffer_if_needed(self):
        if len(self.buffer) == 0:
            return
        self.cache_actor.put_records.remote(self.dataset_name, self.buffer)
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
