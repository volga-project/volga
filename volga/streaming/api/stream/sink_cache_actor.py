from typing import Any, List

import ray


@ray.remote
class SinkCacheActor:

    def __init__(self):
        self.values = []

    def append_value(self, val: Any):
        self.values.append(val)

    def extend_values(self, vals: List[Any]) -> bool:
        self.values.extend(vals)
        return True

    def get_values(self):
        return self.values