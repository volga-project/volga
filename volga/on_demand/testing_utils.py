import asyncio
from typing import Dict, Any, List, Callable, Tuple
from datetime import datetime

import ray
from volga.on_demand.config import OnDemandConfig
from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi, ScyllaPyHotFeatureStorageApi, HotFeatureStorageApiBase
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.api.entity import entity, field
from volga.api.source import source, KafkaSource, Connector

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

TEST_FEATURE_NAME = 'test_feature'

@source(TestEntity)
def test_feature() -> Connector:
    return KafkaSource.mock_with_delayed_items(
        items=[TEST_ENTITY], # this is ignored when we use setup_sample_scylla_feature_data
        delay_s=0
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


def gen_test_entity(i: int) -> TestEntity:
    return TestEntity(
        id=f'test-id-{i}',
        value=i,
        timestamp=datetime.now()
    )


# TODO ideally this should be populated via pipeline job, fix later
@ray.remote
def setup_sample_scylla_feature_data_ray(config: OnDemandConfig, num_keys: int = 1000):
    asyncio.run(setup_sample_scylla_feature_data(config, num_keys))


async def setup_sample_scylla_feature_data(scylla_contact_points: List[str], num_keys: int = 1000):
    api = ScyllaPyHotFeatureStorageApi(scylla_contact_points)
    await api.init()
    await api._drop_tables() # cleanup

    i = 0
    max_tasks = 10000
    futs = set()

    while i < num_keys:
        test_entity = gen_test_entity(i)
        keys = {'id': test_entity.id}
        values = {'value': test_entity.value}

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
