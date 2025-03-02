import asyncio
from typing import Dict, Any, List, Callable, Tuple
from datetime import datetime, timedelta

import ray
from volga.on_demand.config import OnDemandConfig
from volga.storage.scylla.api import ScyllaPyHotFeatureStorageApi, ScyllaFeatureStorageApiBase
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

    BASE_TIME = datetime(2024, 1, 1, 12, 0)

    def __init__(self):
        # Create test data: for each key, store multiple entities with different timestamps
        self.base_time = MockDataConnector.BASE_TIME
        self.test_entities = {
            'test-id': [
                TestEntity(id='test-id', value=1.0, timestamp=self.base_time - timedelta(minutes=2)),
                TestEntity(id='test-id', value=1.5, timestamp=self.base_time - timedelta(minutes=1)),
                TestEntity(id='test-id', value=2.0, timestamp=self.base_time),
            ],
            'test-id-1': [
                TestEntity(id='test-id-1', value=2.0, timestamp=self.base_time - timedelta(minutes=2)),
                TestEntity(id='test-id-1', value=2.5, timestamp=self.base_time - timedelta(minutes=1)),
                TestEntity(id='test-id-1', value=3.0, timestamp=self.base_time),
            ],
            'test-id-2': [
                TestEntity(id='test-id-2', value=3.0, timestamp=self.base_time - timedelta(minutes=2)),
                TestEntity(id='test-id-2', value=3.5, timestamp=self.base_time - timedelta(minutes=1)),
                TestEntity(id='test-id-2', value=4.0, timestamp=self.base_time),
            ],
        }
        
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
        keys: List[Dict[str, Any]]
    ) -> List[Any]:
        """Return latest entity for each key as a flat list"""
        res = [
            self.test_entities.get(key['id'], self.test_entities['test-id'])[-1]  # Get latest entity
            for key in keys
        ]
        return res
        
    async def fetch_range(
        self,
        feature_name: str,
        keys: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> List[List[Any]]:
        """Return all entities within time range for each key as list of lists"""
        results = []
        for key in keys:
            key_entities = self.test_entities.get(key['id'], self.test_entities['test-id'])
            # Filter entities within the time range
            key_results = [
                entity for entity in key_entities
                if start_time <= entity.timestamp <= end_time
            ]
            results.append(key_results)
        return results


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
