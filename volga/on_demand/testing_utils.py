import asyncio
from typing import Dict, Any, Tuple

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
    api = ScyllaPyHotFeatureStorageApi(contact_points)
    await api.init()
    i = 0
    max_tasks = 10000
    futs = set()

    while i < num_keys:
        keys, values = sample_key_value(i)
        if len(futs) == max_tasks:
            done, pending = await asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED)
            for f in done:
                assert f in futs
                futs.remove(f)
        f = asyncio.get_event_loop().create_task(api.insert(TEST_FEATURE_NAME, keys, values))
        futs.add(f)
        i += 1
        if i%1000 == 0:
            print(f'Written {i} sample feature records...')

    await asyncio.gather(*futs)
    print(f'Finished writing sample feature records - {i} total')
