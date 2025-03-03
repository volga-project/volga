import unittest
import ray
from datetime import datetime, timedelta
from volga.api.entity import entity, field

from volga.api.storage import InMemoryActorPipelineDataConnector
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga.common.time_utils import datetime_str_to_ts
import time
from volga.api.schema import Schema
from volga.api.storage import BatchSinkToCacheActorFunction
from volga.storage.common.in_memory_actor import delete_in_memory_cache_actor, get_or_create_in_memory_cache_actor

FEATURE_NAME = 'temperature_feature'

@entity
class TemperatureReading:
    user_id: str = field(key=True)
    timestamp: datetime = field(timestamp=True)
    temperature: float


class TestInMemoryActorE2E(unittest.IsolatedAsyncioTestCase):
    
    @classmethod
    def setUpClass(cls):
        ray.init(ignore_reinit_error=True)
        delete_in_memory_cache_actor()
        
    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def setUp(self):
        self.test_cases = [
            ('stream sink', False),
            ('batch sink', True)
        ]
        
    async def _run_connector_test(self, batch_mode: bool):
        # Get schema from entity metadata
        schema: Schema = TemperatureReading._entity_metadata.schema()
        
        # Initialize connectors
        pipeline_connector = InMemoryActorPipelineDataConnector(batch=batch_mode)
        sink_fn = pipeline_connector.get_sink_function(FEATURE_NAME, schema)
        sink_fn.open(None)
        
        on_demand_connector = InMemoryActorOnDemandDataConnector()
        await on_demand_connector.init()
        
        cache_actor = get_or_create_in_memory_cache_actor()
        ray.get(cache_actor.clear_data.remote())

        try:
            # Generate and write test data
            base_time = datetime(2024, 1, 1, 12, 0)
            base_temp = 20.0
            num_users = 10
            num_records_per_user = 5
            test_data = {
                f'user{i}': [
                    TemperatureReading(
                        user_id=f'user{i}',
                        timestamp=base_time + timedelta(minutes=j),
                        temperature=base_temp + j
                    )
                    for j in range(num_records_per_user)
                ]
                for i in range(num_users)
            }
            
            for user_id, records in test_data.items():
                for record in records:
                    sink_fn.sink(record.__dict__)
            
            # Wait for data to be recorded
            time.sleep(BatchSinkToCacheActorFunction.DUMPER_PERIOD_S + 1)
            
            # Test latest
            latest = await on_demand_connector.fetch_latest(
                FEATURE_NAME,
                [
                    {'user_id': user_id}
                    for user_id in test_data.keys()
                ]
            )

            self.assertEqual(len(latest), num_users)
            for i in range(len(latest)):
                latest_reading = TemperatureReading(**latest[i][0])
                user_id = latest_reading.user_id
                latest_test_data_per_user = test_data[user_id][-1]
                self.assertEqual(latest_reading.temperature, latest_test_data_per_user.temperature)
                self.assertEqual(latest_reading.user_id, latest_test_data_per_user.user_id)
                self.assertEqual(latest_reading.timestamp, latest_test_data_per_user.timestamp)
            
            # Test range
            user_id = list(test_data.keys())[0]
            start_ts = datetime_str_to_ts(test_data[user_id][0].timestamp.isoformat())
            end_ts = datetime_str_to_ts(test_data[user_id][-1].timestamp.isoformat())
            
            range_data = await on_demand_connector.fetch_range(
                FEATURE_NAME,
                [
                    {'user_id': user_id}
                    for user_id in test_data.keys()
                ],
                start_ts,
                end_ts
            )

            self.assertEqual(len(latest), num_users)
            for i in range(len(range_data)):
                range_reading = TemperatureReading(**range_data[i][0])
                first_user_id = range_reading.user_id
                self.assertEqual(len(range_data[i]), len(test_data[first_user_id]))
                for j in range(len(range_data[i])):
                    data_per_user = TemperatureReading(**range_data[i][j])
                    test_data_per_user = test_data[first_user_id][j]
                    self.assertEqual(data_per_user.temperature, test_data_per_user.temperature)
                    self.assertEqual(data_per_user.user_id, test_data_per_user.user_id)
                    self.assertEqual(data_per_user.timestamp, test_data_per_user.timestamp)
            
        finally:
            await on_demand_connector.close()
            sink_fn.close()

    async def test_stream_and_batch_modes(self):
        for mode_name, batch_mode in self.test_cases:
            with self.subTest(mode=mode_name):
                await self._run_connector_test(batch_mode)


if __name__ == '__main__':
    unittest.main()
