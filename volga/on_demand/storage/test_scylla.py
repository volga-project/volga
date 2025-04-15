import asyncio
import unittest
from datetime import datetime
from typing import List

import ray

from volga.api.feature import FeatureRepository
from volga.on_demand.storage.scylla import OnDemandScyllaConnector
from volga.on_demand.testing_utils import (
    TEST_FEATURE_NAME, TestEntity, setup_sample_scylla_feature_data
)

# Mock Scylla contact points - replace with appropriate test values
SCYLLA_CONTACT_POINTS = ['127.1.29.1']
# SCYLLA_CONTACT_POINTS = ['scylla-client.scylla-operator.svc.cluster.local']
NUM_TEST_KEYS = 10


class TestOnDemandScyllaConnector(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up test data and connector once for all tests"""
        # Run setup in event loop
        cls.loop = asyncio.get_event_loop()
        cls.loop.run_until_complete(setup_sample_scylla_feature_data(SCYLLA_CONTACT_POINTS, NUM_TEST_KEYS))
        
        # Create and initialize connector
        cls.connector = OnDemandScyllaConnector(contact_points=SCYLLA_CONTACT_POINTS)
        cls.loop.run_until_complete(cls.connector.init())
        
        # Register the test feature to the FeatureRepository
        # This is normally done by the @source decorator
        FeatureRepository._features[TEST_FEATURE_NAME] = type('MockFeature', (), {
            'output_type': TestEntity,
            'name': TEST_FEATURE_NAME
        })
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        cls.loop.run_until_complete(cls.connector.close())
        
    def test_fetch_latest(self):
        """Test fetching latest data for multiple keys"""
        keys = [{'id': f'test-id-{i}'} for i in range(3)]
        result = self.loop.run_until_complete(self.connector.fetch_latest(TEST_FEATURE_NAME, keys))
        
        # Verify the results
        self.assertEqual(len(result), 3)
        for i, entities in enumerate(result):
            self.assertEqual(len(entities), 1)
            entity = entities[0]
            self.assertEqual(entity['id'], f'test-id-{i}')
            self.assertEqual(entity['value'], float(i))
            self.assertIsInstance(entity['timestamp'], datetime)

if __name__ == '__main__':
    unittest.main()
