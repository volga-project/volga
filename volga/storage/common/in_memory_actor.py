import bisect
from typing import Dict, Any, Optional, List, Tuple

import ray
from decimal import Decimal
from volga.storage.common.key_index import compose_main_key, KeyIndex


CACHE_ACTOR_NAME = 'cache_actor'

@ray.remote(num_cpus=0.001)
class InMemoryCacheActor:
    def __init__(self):
        self.per_feature_per_key: Dict[str, Dict[str, List[Tuple[Decimal, Any]]]] = {}
        self.key_index_per_feature: Dict[str, KeyIndex] = {}

    def ready(self) -> bool:
        return True

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
        keys_dict: Optional[Dict[str, Any]],
        start: Optional[Decimal], end: Optional[Decimal],
        with_timestamps: bool = False
    ) -> List:
        if feature_name not in self.per_feature_per_key:
            raise RuntimeError(f'No feature {feature_name}')

        if keys_dict is not None:
            possible_keys = self.key_index_per_feature[feature_name].get(keys_dict)
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

    def get_latest(self, feature_name: str, keys_dict: Dict[str, Any]) -> Optional[List]:
        vals = self.get_range(feature_name=feature_name, keys_dict=keys_dict, start=None, end=None, with_timestamps=True)
        if len(vals) == 0:
            return None

        last = len(vals) - 1
        last_ts = vals[last][0]
        res = []
        while last >= 0 and vals[last][0] == last_ts:
            res.append(vals[last][1])
            last -= 1
        return res

