import asyncio
from typing import Dict, Any, List, Tuple

import ray
from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand_config import OnDemandConfig
from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi, ScyllaPyHotFeatureStorageApi, HotFeatureStorageApiBase

TEST_FEATURE_NAME = 'test_feature'


def sample_key_value(i: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return {'key': f'key_{i}'}, {'value': f'value_{i}'}


@ray.remote
def setup_sample_feature_data_ray(config: OnDemandConfig, num_keys: int = 1000):
    asyncio.run(setup_sample_feature_data(config, num_keys))

async def setup_sample_feature_data(config: OnDemandConfig, num_keys: int = 1000):
    await DataService._cleanup_db(config.data_service_config)
    contact_points = config.data_service_config['scylla']['contact_points']
    # api = AcsyllaHotFeatureStorageApi(contact_points)
    api = ScyllaPyHotFeatureStorageApi(contact_points)
    keys_values = [sample_key_value(i) for i in range(num_keys)]

    await _insert_data(api, keys_values)


async def _insert_data(api: HotFeatureStorageApiBase, keys_values: List[Tuple[Dict[str, Any], Dict[str, Any]]]):
    await api.init()
    futs = []
    for (keys, values) in keys_values:
        futs.append(api.insert(TEST_FEATURE_NAME, keys, values))
    await asyncio.gather(*futs)