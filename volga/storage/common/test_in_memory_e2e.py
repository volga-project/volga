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
        
        try:
            # Generate and write test data
            base_time = datetime(2024, 1, 1, 12, 0)
            test_data = [
                TemperatureReading(
                    user_id='user1',
                    timestamp=base_time + timedelta(minutes=i),
                    temperature=20.0 + i
                )
                for i in range(5)
            ]
            
            for record in test_data:
                sink_fn.sink(record.__dict__)
            
            # Wait for data to be recorded
            time.sleep(BatchSinkToCacheActorFunction.DUMPER_PERIOD_S + 1)
            
            # Test latest
            latest = await on_demand_connector.fetch_latest(
                FEATURE_NAME,
                {'user_id': 'user1'}
            )
            latest_reading = TemperatureReading(**latest)
            
            self.assertEqual(latest_reading.temperature, 24.0)
            self.assertEqual(latest_reading.user_id, 'user1')
            self.assertEqual(latest_reading.timestamp, test_data[4].timestamp)
            
            # Test range
            start_ts = datetime_str_to_ts(test_data[1].timestamp.isoformat())
            end_ts = datetime_str_to_ts(test_data[3].timestamp.isoformat())
            
            range_data = await on_demand_connector.fetch_range(
                FEATURE_NAME,
                {'user_id': 'user1'},
                start_ts,
                end_ts
            )
            
            range_readings = [TemperatureReading(**data) for data in range_data]
            
            self.assertEqual(len(range_readings), 3)
            self.assertEqual(range_readings[0].temperature, 21.0)
            self.assertEqual(range_readings[-1].temperature, 23.0)
            
        finally:
            await on_demand_connector.close()
            sink_fn.close()

    async def test_stream_and_batch_modes(self):
        for mode_name, batch_mode in self.test_cases:
            with self.subTest(mode=mode_name):
                await self._run_connector_test(batch_mode)


if __name__ == '__main__':
    unittest.main()
