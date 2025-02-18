import unittest
import asyncio
from datetime import datetime
import json
from typing import List, Dict, Any
from unittest.mock import AsyncMock, patch

from volga.api.entity import entity, field
from volga.api.feature import Feature, FeatureRepository
from volga.api.source import source, KafkaSource, Connector
from volga.api.pipeline import PipelineFeature
from volga.api.on_demand import on_demand, OnDemandFeature
from volga.on_demand.executor import OnDemandExecutor
from volga.on_demand.data.data_service import DataService


# Test entities
@entity
class TestEntity:
    id: str = field(key=True)
    value: float
    timestamp: datetime = field(timestamp=True)


@entity
class DependentEntity:
    id: str = field(key=True)
    computed_value: float
    timestamp: datetime = field(timestamp=True)


test_entity = TestEntity(
    id='test-id',
    value=1.0,
    timestamp=datetime.now()
)

@source(TestEntity)
def pipeline_feature() -> Connector:
    return KafkaSource.mock_with_delayed_items(
        items=[test_entity],
        delay_s=0
    )


# Define on-demand features
@on_demand(dependencies=['pipeline_feature'])
def simple_feature(
    dep: TestEntity,
    multiplier: float = 1.0
) -> DependentEntity:
    """Simple on-demand feature"""
    return DependentEntity(
        id=dep.id,
        computed_value=dep.value * multiplier,
        timestamp=datetime.now()
    )


@on_demand(dependencies=['simple_feature'])
def chained_feature(
    dep: DependentEntity,
    offset: float = 0.0
) -> DependentEntity:
    """Feature that depends on another on-demand feature"""
    return DependentEntity(
        id=dep.id,
        computed_value=dep.computed_value + offset,
        timestamp=datetime.now()
    )

class TestOnDemandExecutor(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method"""
        
        # Mock DataService
        self.data_service_patcher = patch('volga.on_demand.executor.DataService')
        self.mock_data_service = self.data_service_patcher.start()
        
        # Set up async mock for fetch_latest with standard test data
        self.mock_fetch_latest = AsyncMock()
        self.test_entity = TestEntity(
            id='test-id',
            value=1.0,
            timestamp=datetime.now()
        )
        self.mock_fetch_latest.return_value = self.test_entity
        self.mock_data_service.fetch_latest = self.mock_fetch_latest

    def tearDown(self):
        """Clean up after each test"""
        self.data_service_patcher.stop()

    async def async_test_source_feature_execution(self):
        """Test execution of source features"""
        executor = OnDemandExecutor()
        result = await executor._fetch_pipeline_feature(
            'pipeline_feature',
            {'id': 'test-id'},
            TestEntity
        )
        
        # Verify result
        self.assertIsInstance(result, TestEntity)
        self.assertEqual(result.id, 'test-id')
        self.assertEqual(result.value, 1.0)

    def test_source_feature_execution(self):
        asyncio.run(self.async_test_source_feature_execution())

    async def async_test_full_execution(self):
        """Test full execution flow with multiple features"""
        executor = OnDemandExecutor()
        results = await executor.execute(
            target_features=['chained_feature'],
            feature_keys={
                'pipeline_feature': {'id': 'test-id'},
            },
            udf_args={
                'simple_feature': {'multiplier': 2.0},
                'chained_feature': {'offset': 1.0}
            }
        )
        
        # Verify results
        self.assertIn('chained_feature', results)
        result = results['chained_feature']
        self.assertIsInstance(result, DependentEntity)
        self.assertEqual(result.id, 'test-id')
        self.assertEqual(result.computed_value, 3.0)  # (1.0 * 2.0) + 1.0

    def test_full_execution(self):
        asyncio.run(self.async_test_full_execution())


    def test_executor_initialization(self):
        """Test executor initialization and feature categorization"""
        # Get all features
        executor = OnDemandExecutor()
        features = FeatureRepository.get_all_features()
        pipeline_feature = features['pipeline_feature']
        
        # Check on-demand features
        self.assertIn('simple_feature', executor._ondemand_features)
        self.assertIn('chained_feature', executor._ondemand_features)
        self.assertNotIn('pipeline_feature', executor._ondemand_features)
        
        # Check pipeline dependencies
        self.assertIn('pipeline_feature', executor._pipeline_deps)
        
        # Verify pipeline feature is actually in dependencies
        simple_feature = executor._ondemand_features['simple_feature']
        self.assertIn(pipeline_feature, simple_feature.dependencies)
        
        # Check dependency graph (which uses feature names)
        expected_graph = {
            'simple_feature': {'pipeline_feature'},
            'chained_feature': {'simple_feature'}
        }
        self.assertEqual(executor._dependency_graph, expected_graph)


    def test_get_execution_order(self):
        """Test that _get_execution_order returns correct execution levels"""
        
        # Define features with different dependency patterns
        
        @source(TestEntity)
        def base_feature1() -> Connector:
            return KafkaSource.mock_with_delayed_items(
                items=[self.test_entity],
                delay_s=0
            )

        @source(TestEntity)
        def base_feature2() -> Connector:
            return KafkaSource.mock_with_delayed_items(
                items=[self.test_entity],
                delay_s=0
            )

        @on_demand(dependencies=['base_feature1'])
        def level1_feature1(dep: TestEntity) -> DependentEntity:
            return DependentEntity(
                id=dep.id,
                computed_value=dep.value,
                timestamp=datetime.now()
            )

        @on_demand(dependencies=['base_feature2'])
        def level1_feature2(dep: TestEntity) -> DependentEntity:
            return DependentEntity(
                id=dep.id,
                computed_value=dep.value,
                timestamp=datetime.now()
            )

        @on_demand(dependencies=['level1_feature1', 'level1_feature2'])
        def level2_feature(dep1: DependentEntity, dep2: DependentEntity) -> DependentEntity:
            return DependentEntity(
                id=dep1.id,
                computed_value=dep1.computed_value + dep2.computed_value,
                timestamp=datetime.now()
            )

        @on_demand(dependencies=['level2_feature'])
        def level3_feature(dep: DependentEntity) -> DependentEntity:
            return DependentEntity(
                id=dep.id,
                computed_value=dep.computed_value,
                timestamp=datetime.now()
            )

        # Create new executor with test features
        executor = OnDemandExecutor()

        # Test different execution order scenarios
        
        # Scenario 1: Single feature with dependencies
        execution_order = executor._get_execution_order(['level2_feature'])
        self.assertEqual(len(execution_order), 3)  # Should have 3 levels
        self.assertEqual(set(execution_order[0]), {'base_feature1', 'base_feature2'})
        self.assertEqual(set(execution_order[1]), {'level1_feature1', 'level1_feature2'})
        self.assertEqual(execution_order[2], ['level2_feature'])

        # Scenario 2: Multiple target features at different levels
        execution_order = executor._get_execution_order(['level2_feature', 'level1_feature1'])
        self.assertEqual(len(execution_order), 3)  # Should have 3 levels
        self.assertEqual(set(execution_order[0]), {'base_feature1', 'base_feature2'})
        self.assertEqual(set(execution_order[1]), {'level1_feature1', 'level1_feature2'})
        self.assertEqual(execution_order[2], ['level2_feature'])

        # Scenario 3: Full dependency chain
        execution_order = executor._get_execution_order(['level3_feature'])
        self.assertEqual(len(execution_order), 4)  # Should have 4 levels
        self.assertEqual(set(execution_order[0]), {'base_feature1', 'base_feature2'})
        self.assertEqual(set(execution_order[1]), {'level1_feature1', 'level1_feature2'})
        self.assertEqual(execution_order[2], ['level2_feature'])
        self.assertEqual(execution_order[3], ['level3_feature'])

        # Scenario 4: Independent features
        execution_order = executor._get_execution_order(['base_feature1', 'base_feature2'])
        self.assertEqual(len(execution_order), 1)  # Should have 1 level
        self.assertEqual(set(execution_order[0]), {'base_feature1', 'base_feature2'})


if __name__ == '__main__':
    unittest.main()