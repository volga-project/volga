import bisect
from threading import Thread
from typing import Dict, Any, Optional, List, Tuple

import ray
from decimal import Decimal
from ray.actor import ActorHandle
import time

from volga.common.time_utils import datetime_str_to_ts
from volga.api.dataset.schema import Schema
from volga.storage.cold.cold import ColdStorage
from volga.storage.common.key_index import compose_main_key, KeyIndex
from volga.storage.hot.hot import HotStorage
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SinkFunction


class SimpleInMemoryActorStorage(ColdStorage, HotStorage):

    CACHE_ACTOR_NAME = 'cache_actor'

    def __init__(self):
        self.cache_actor = SimpleInMemoryCacheActor.options(name=self.CACHE_ACTOR_NAME, get_if_exists=True).remote()

    # TODO dataset_name and schema should be part of all records metadata
    #  When it is so, these outside params won't be needed
    def gen_sink_function(self, dataset_name: str, output_schema: Schema, hot: bool) -> SinkFunction:
        if hot:
            return SingleEventSinkToCacheActorFunction(
                cache_actor=self.cache_actor,
                dataset_name=dataset_name,
                output_schema=output_schema
            )
        else:
            return BulkSinkToCacheActorFunction(
                cache_actor=self.cache_actor,
                dataset_name=dataset_name,
                output_schema=output_schema
            )

    def get_data(self, dataset_name: str, keys: Optional[List[Dict[str, Any]]], start_ts: Optional[Decimal], end_ts: Optional[Decimal]) -> List[Any]:
        if keys is not None:
            if len(keys) > 1:
                raise ValueError('Multiple key lookup is not supported yet')
            keys_dict = keys[0]
        else:
            keys_dict = None
        return ray.get(self.cache_actor.get_values.remote(dataset_name, keys_dict, start_ts, end_ts))

    def get_latest_data(self, dataset_name: str, keys: List[Dict[str, Any]]) -> Optional[List[Any]]:
        if keys is not None:
            if len(keys) > 1:
                raise ValueError('Multiple key lookup is not supported yet')
            keys_dict = keys[0]
        else:
            keys_dict = None

        return ray.get(self.cache_actor.get_latest.remote(dataset_name, keys_dict))


@ray.remote(num_cpus=0.01)# TODO set memory request
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

            # ts_vals.append((ts, value))
            bisect.insort_right(ts_vals, (ts, value))

    def get_values(
        self,
        dataset_name: str,
        keys_dict: Optional[Dict[str, Any]],
        start: Optional[Decimal], end: Optional[Decimal],
        with_timestamps: bool = False
    ) -> List:
        if dataset_name not in self.per_dataset_per_key:
            raise RuntimeError(f'No dataset {dataset_name}')

        if keys_dict is not None:
            possible_keys = self.key_index_per_dataset[dataset_name].get(keys_dict)
        else:
            possible_keys = list(self.per_dataset_per_key[dataset_name].keys())

        res = []

        for key in possible_keys:
            timestamped_values = self.per_dataset_per_key[dataset_name][key]
            # range query on sorted list
            first = 0
            while start is not None and timestamped_values[first][0] < start:
                first += 1

            last = len(timestamped_values) - 1
            while end is not None and timestamped_values[last][0] > end:
                last -= 1

            values = timestamped_values[first: last + 1]

            # TODO we can merge those since both arrays are sorted
            res.extend(values)

        res.sort(key=lambda e: e[0])
        if with_timestamps:
            return res
        else:
            return list(map(lambda e: e[1], res)) # remove timestamps

    def get_latest(self, dataset_name: str, keys_dict: Dict[str, Any]) -> Optional[List]:
        vals = self.get_values(dataset_name=dataset_name, keys_dict=keys_dict, start=None, end=None, with_timestamps=True)
        if len(vals) == 0:
            return None

        last = len(vals) - 1
        last_ts = vals[last][0]
        res = []
        while last >= 0 and vals[last][0] == last_ts:
            res.append(vals[last][1])
            last -= 1
        return res


class BulkSinkToCacheActorFunction(SinkFunction):

    DUMPER_PERIOD_S = 1

    # TODO we need to get rid of passing output_schema, it should be a part of a message
    def __init__(self, cache_actor: ActorHandle, dataset_name: str, output_schema: Schema):
        self.cache_actor = cache_actor
        self.dataset_name = dataset_name
        self.output_schema = output_schema
        self.buffer = []
        self.dumper_thread = None
        self.running = False

    def sink(self, value):
        # print(value)
        # raise
        # TODO all of this should be a part of Record object (value) and not sourced from outside
        key_fields = list(self.output_schema.keys.keys())
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


class SingleEventSinkToCacheActorFunction(SinkFunction):

    def __init__(self, cache_actor: ActorHandle, dataset_name: str, output_schema: Schema):
        self.cache_actor = cache_actor
        self.dataset_name = dataset_name
        self.output_schema = output_schema

    def sink(self, value):
        key_fields = list(self.output_schema.keys.keys())
        keys_dict = {k: value[k] for k in key_fields}
        timestamp_field = self.output_schema.timestamp
        ts = datetime_str_to_ts(value[timestamp_field])
        self.cache_actor.put_records.remote(self.dataset_name, [(keys_dict, ts, value)])

