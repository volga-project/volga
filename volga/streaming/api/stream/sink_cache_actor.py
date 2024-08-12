from typing import Any, List, Dict

import ray


@ray.remote
class SinkCacheActor:

    def __init__(self, ):
        self._list = []
        self._dict = {}

    def append_to_list(self, val: Any):
        self._list.append(val)

    def extend_list(self, vals: List[Any]):
        self._list.extend(vals)

    def get_list(self) -> List:
        return self._list

    def extend_dict(self, d: Dict):
        for k in d:
            self._dict[k] = d[k]

    def get_dict(self) -> Dict:
        return self._dict


