from volga.on_demand.testing_utils import TEST_ENTITY, TestEntity, MockDataConnector
from volga.on_demand.config import OnDemandConfig, OnDemandDataConnectorConfig  
from volga.on_demand.client import OnDemandClient
from volga.on_demand.models import OnDemandRequest, OnDemandResponse
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.api.source import source, KafkaSource, Connector
from volga.api.on_demand import on_demand
from datetime import datetime
import asyncio
import ray
import time
from typing import Dict, Any
import unittest
from volga.api.feature import FeatureRepository


@source(TestEntity)
def pipeline_feature() -> Connector:
    return KafkaSource.mock_with_delayed_items(
        items=[TEST_ENTITY],
        delay_s=0
    )


@on_demand(dependencies=['pipeline_feature'])
def simple_feature(
    dep: TestEntity,
    multiplier: float = 1.0
) -> TestEntity:
    """Simple on-demand feature that multiplies the value"""
    return TestEntity(
        id=dep.id,
        value=dep.value * multiplier,
        timestamp=datetime.now()
    )


class TestOnDemandServing(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def _cast_to_test_entity(self, result: Dict[str, Any]) -> TestEntity:
        """Helper method to cast response dict to TestEntity"""
        try:
            return TestEntity(
                id=result['id'],
                value=result['value'],
                timestamp=datetime.fromisoformat(result['timestamp'])
            )
        except (KeyError, ValueError) as e:
            self.fail(f"Failed to cast response to TestEntity: {e}")

    def test_serving(self):
        # Create config
        SERVER_PORT = 1122
        CLIENT_URL = f'http://127.0.0.1:{SERVER_PORT}'

        config = OnDemandConfig(
            num_servers_per_node=2,
            server_port=1122,
            data_connector=OnDemandDataConnectorConfig(
                connector_class=MockDataConnector,
                connector_args={}
            )
        )

        # Create request
        request = OnDemandRequest(
            target_features=['simple_feature'],
            feature_keys={
                'simple_feature': {'id': 'test-id'}
            },
            udf_args={
                'simple_feature': {'multiplier': 2.0}
            }
        )

        # Initialize Ray and create coordinator
        with ray.init():
            coordinator = create_on_demand_coordinator(config)
            
            # Start servers
            servers_per_node = ray.get(coordinator.start.remote())
            print(f"Started {servers_per_node} servers")

            features = FeatureRepository.get_all_features()

            ray.get(coordinator.register_features.remote(features))

            # Wait for servers to be ready
            time.sleep(1)

            # Create client
            client = OnDemandClient(CLIENT_URL)
            try:
                # Test single request
                response: OnDemandResponse = self.loop.run_until_complete(client.request(request))

                # Verify single response
                self.assertIn('simple_feature', response.results)
                result = response.results['simple_feature']
                
                # Verify response can be cast to TestEntity
                entity = self._cast_to_test_entity(result)
                self.assertIsInstance(entity, TestEntity)
                self.assertEqual(entity.id, 'test-id')
                self.assertEqual(entity.value, 2.0)  # 1.0 * 2.0
                
                self.assertIsInstance(response.server_id, int)

                # Test multiple requests
                num_requests = 100
                requests = [request for _ in range(num_requests)]
                responses = self.loop.run_until_complete(client.request_many(requests))

                # Verify results and type casting
                for response in responses:
                    result = response.results['simple_feature']
                    entity = self._cast_to_test_entity(result)
                    self.assertIsInstance(entity, TestEntity)
                    self.assertEqual(entity.id, 'test-id')
                    self.assertEqual(entity.value, 2.0)

                print(f"Successfully processed {num_requests}")

            finally:
                # Close client
                self.loop.run_until_complete(client.close())


if __name__ == '__main__':
    unittest.main()