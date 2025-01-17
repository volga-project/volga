import asyncio
from typing import Dict, Any, List, Tuple

from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi

TEST_FEATURE_NAME = 'test_feature'


def sample_key_value(i: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return {'key': f'key_{i}'}, {'value': f'value_{i}'}


def create_sample_feature_data(num_keys: int = 1000):
    api = AcsyllaHotFeatureStorageApi()
    keys_values = [sample_key_value(i) for i in range(num_keys)]

    asyncio.get_event_loop().run_until_complete(_insert_data(api, keys_values))


async def _insert_data(api: AcsyllaHotFeatureStorageApi, keys_values: List[Tuple[Dict[str, Any], Dict[str, Any]]]):
    await api.init()
    futs = []
    for (keys, values) in keys_values:
        futs.append(api.insert(TEST_FEATURE_NAME, keys, values))
    await asyncio.gather(*futs)