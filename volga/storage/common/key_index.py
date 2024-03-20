import itertools
from typing import Dict, List, Any, Set

DEFAULT_KV_SEPARATOR = '='
DEFAULT_KEYS_SEPARATOR = '-'


# this should be done with B-tree, but this will work for now
class KeyIndex:

    def __init__(self):
        self.index: Dict[str, Set[str]] = {}

    def put(self, keys_dict: Dict[str, Any]):
        main_key = compose_main_key(keys_dict)
        possible_keys = compose_possible_keys(keys_dict)
        for k in possible_keys:
            if k in self.index:
                keys = self.index[k]
                keys.add(main_key)
            else:
                keys = {main_key}
            self.index[k] = keys

    def get(self, keys_dict: Dict[str, Any]) -> List[str]:
        main_key = compose_main_key(keys_dict)
        return list(self.index.get(main_key, []))


def compose_possible_keys(keys_dict: Dict[str, Any]) -> List[str]:
    kv_strings = _kv_strings(keys_dict)
    depth = len(keys_dict)
    if depth < 2:
        return kv_strings
    res = set(kv_strings)
    for d in range(2, depth):
        combs = list(itertools.combinations(kv_strings, d))
        for comb in combs:
            comb = list(comb)
            comb.sort() # idempotency
            key = DEFAULT_KEYS_SEPARATOR.join(comb)
            res.add(key)
    return list(res)


# TODO this should be idempotent, test
def compose_main_key(keys_dict: Dict[str, Any]) -> str:
    kv_strings = _kv_strings(keys_dict)
    kv_strings.sort() # for idempotency
    return DEFAULT_KEYS_SEPARATOR.join(kv_strings)


def _kv_strings(d: Dict) -> List[str]:
    # TODO validate value can be stringified
    return [(lambda k, v: f'{k}{DEFAULT_KV_SEPARATOR}{v}')(k, v) for k, v in zip(list(d.keys()), list(d.values()))]
