import bisect
from pprint import pprint
from typing import Dict, Any, Optional, List, Tuple

import ray
from decimal import Decimal
from volga.common.ray.resource_manager import ResourceManager
from volga.storage.common.key_index import compose_main_key, KeyIndex
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

CACHE_ACTOR_NAME = 'cache_actor'

@ray.remote(num_cpus=0)
class InMemoryCacheActor:
    def __init__(self):
        self.per_feature_per_key: Dict[str, Dict[str, List[Tuple[Decimal, Any]]]] = {}
        self.key_index_per_feature: Dict[str, KeyIndex] = {}

    def ready(self) -> bool:
        return True
    
    def get_all(self) -> Dict[str, Dict[str, List[Tuple[Decimal, Any]]]]:
        return self.per_feature_per_key

    def put_records(self, feature_name: str, records: List[Tuple[Dict[str, Any], Decimal, Any]]):
        if feature_name in self.per_feature_per_key:
            per_key = self.per_feature_per_key[feature_name]
            key_index = self.key_index_per_feature[feature_name]
        else:
            per_key = {}
            key_index = KeyIndex()
            self.per_feature_per_key[feature_name] = per_key
            self.key_index_per_feature[feature_name] = key_index

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

    def get_range(
        self,
        feature_name: str,
        keys: List[Dict[str, Any]],
        start: Optional[Decimal], end: Optional[Decimal],
        with_timestamps: bool = False
    ) -> List[Optional[List[Any]]]:
        res = []
        for key in keys:
            res.append(self.get_range_per_key(feature_name=feature_name, key=key, start=start, end=end, with_timestamps=with_timestamps))
        return res
    
    def get_latest(
        self,
        feature_name: str,
        keys: List[Dict[str, Any]],
    ) -> List[Optional[List[Any]]]:
        res = []
        for key in keys:
            res.append(self.get_latest_per_key(feature_name=feature_name, key=key))
        return res

    def get_range_per_key(
        self,
        feature_name: str,
        key: Optional[Dict[str, Any]],
        start: Optional[Decimal], end: Optional[Decimal],
        with_timestamps: bool = False
    ) -> Optional[List[Any]]:
        if feature_name not in self.per_feature_per_key:
            # raise RuntimeError(f'No feature {feature_name} in {self.per_feature_per_key.keys()}')
            # return []
            return None

        if key is not None:
            possible_keys = self.key_index_per_feature[feature_name].get(key)
        else:
            possible_keys = list(self.per_feature_per_key[feature_name].keys())

        res = []

        for key in possible_keys:
            timestamped_values = self.per_feature_per_key[feature_name][key]
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

    def get_latest_per_key(self, feature_name: str, key: Dict[str, Any]) -> Optional[List[Any]]:
        vals = self.get_range_per_key(feature_name=feature_name, key=key, start=None, end=None, with_timestamps=True)
        if vals is None:
            return None

        if len(vals) == 0:
            return []

        last = len(vals) - 1
        last_ts = vals[last][0]
        res = []
        while last >= 0 and vals[last][0] == last_ts:
            res.append(vals[last][1])
            last -= 1
        return res
    
    def clear_data(self):
        self.per_feature_per_key = {}
        self.key_index_per_feature = {}

def get_or_create_in_memory_cache_actor() -> ActorHandle:
    options_kwargs = {
        'name': CACHE_ACTOR_NAME,
        'get_if_exists': True,
        'num_cpus': 0,
        'lifetime': 'detached',
        'scheduling_strategy': NodeAffinitySchedulingStrategy(
            node_id=ResourceManager.fetch_head_node().node_id,
            soft=False
        )
    }
    cache_actor = InMemoryCacheActor.options(**options_kwargs).remote()
    return cache_actor

def delete_in_memory_cache_actor() -> None:
    actor = get_or_create_in_memory_cache_actor()
    ray.kill(actor)
