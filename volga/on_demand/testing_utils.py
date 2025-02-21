import asyncio
from typing import Dict, Any, List, Callable
from datetime import datetime

import ray
from volga.on_demand.config import OnDemandConfig
from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi, ScyllaPyHotFeatureStorageApi, HotFeatureStorageApiBase
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.api.entity import entity, field


@entity
class TestEntity:
    id: str = field(key=True)
    value: float
    timestamp: datetime = field(timestamp=True)

TEST_ENTITY = TestEntity(
    id='test-id',
    value=1.0,
    timestamp=datetime.now()
)

# Mock data connector for testing
class MockDataConnector(OnDemandDataConnector):
    def __init__(self):
        self.test_entity = TEST_ENTITY
        
    def query_dict(self) -> Dict[str, Callable]:
        return {
            'latest': self.fetch_latest,
            'range': self.fetch_range
        }
        
    async def init(self):
        pass
        
    async def close(self):
        pass
        
    async def fetch_latest(
        self,
        feature_name: str,
        keys: Dict[str, Any]
    ) -> Any:
        return self.test_entity
        
    async def fetch_range(
        self,
        feature_name: str,
        keys: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> List[Any]:
        return [self.test_entity]

# TEST_FEATURE_NAME = 'test_feature'


# def sample_key_value(i: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#     return {'key': f'key_{i}'}, {'value': f'value_{i}'}


# # @ray.remote
# def setup_sample_feature_data_ray(config: OnDemandConfig, num_keys: int = 1000):
#     asyncio.run(setup_sample_feature_data(config, num_keys))


# async def setup_sample_feature_data(config: OnDemandConfig, num_keys: int = 1000):
#     await DataService._cleanup_db(config.data_service_config)
#     contact_points = config.data_service_config['scylla']['contact_points']
#     api = ScyllaPyHotFeatureStorageApi(contact_points)
#     await api.init()
#     i = 0
#     max_tasks = 10000
#     futs = set()

#     while i < num_keys:
#         keys, values = sample_key_value(i)
#         if len(futs) == max_tasks:
#             done, pending = await asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED)
#             for f in done:
#                 assert f in futs
#                 futs.remove(f)
#         f = asyncio.get_event_loop().create_task(api.insert(TEST_FEATURE_NAME, keys, values))
#         futs.add(f)
#         i += 1
#         if i%1000 == 0:
#             print(f'Written {i} sample feature records...')

#     await asyncio.gather(*futs)
#     print(f'Finished writing sample feature records - {i} total')
