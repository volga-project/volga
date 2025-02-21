import unittest
import asyncio
from datetime import datetime
import json
from typing import List, Dict, Any, Callable

from volga.api.entity import entity, field
from volga.api.feature import Feature, FeatureRepository
from volga.api.source import source, KafkaSource, Connector
from volga.api.pipeline import PipelineFeature
from volga.api.on_demand import on_demand, OnDemandFeature
from volga.on_demand.executor import OnDemandExecutor
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.on_demand.models import OnDemandRequest
from volga.on_demand.testing_utils import TEST_ENTITY, TestEntity, MockDataConnector

@entity
class DependentEntity:
    id: str = field(key=True)
    computed_value: float
    timestamp: datetime = field(timestamp=True)


@source(TestEntity)
def pipeline_feature() -> Connector:
    return KafkaSource.mock_with_delayed_items(
        items=[TEST_ENTITY],
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

@on_demand([('pipeline_feature', 'range')])
def list_feature(
    entities: List[TestEntity]
) -> DependentEntity:
    total = sum(e.value for e in entities)
    return DependentEntity(
        id=entities[0].id if entities else 'default',
        computed_value=total,
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
        self.executor = OnDemandExecutor(MockDataConnector())
        self.executor.register_features(FeatureRepository.get_all_features())

    async def async_test_source_feature_execution(self):
        """Test execution of source features"""
        result = await self.executor._fetch_pipeline_feature(
            'pipeline_feature',
            {'id': TEST_ENTITY.id},
            TestEntity,
            'latest'
        )
        
        # Verify result
        self.assertIsInstance(result, TestEntity)
        self.assertEqual(result.id, TEST_ENTITY.id)
        self.assertEqual(result.value, TEST_ENTITY.value)

    def test_source_feature_execution(self):
        asyncio.run(self.async_test_source_feature_execution())

    async def async_test_chained_execution(self):
        """Test full execution flow with multiple features"""
        request = OnDemandRequest(
            target_features=['chained_feature'],
            feature_keys={
                'simple_feature': {'id': 'test-id'},
            },
            udf_args={
                'simple_feature': {'multiplier': 2.0},
                'chained_feature': {'offset': 1.0}
            }
        )
        
        results = await self.executor.execute(request)
        
        # Verify results
        self.assertIn('chained_feature', results)
        result = results['chained_feature']
        self.assertIsInstance(result, DependentEntity)
        self.assertEqual(result.computed_value, 3.0)  # (1.0 * 2.0) + 1.0

    def test_chained_execution(self):
        asyncio.run(self.async_test_chained_execution())

    async def async_test_multiple_queries(self):
        """Test executing features that use same pipeline feature with different queries"""
        request = OnDemandRequest(
            target_features=['simple_feature', 'list_feature'],
            feature_keys={
                'simple_feature': {'id': 'test-id'},
                'list_feature': {'id': 'test-id'}
            },
            query_args={
                'list_feature': {
                    'start_time': datetime.now(),
                    'end_time': datetime.now()
                }
            },
            udf_args={
                'simple_feature': {'multiplier': 2.0}
            }
        )
        
        results = await self.executor.execute(request)
        
        # Verify both features executed correctly
        self.assertIn('simple_feature', results)
        self.assertIn('list_feature', results)
        
        # Check simple_feature result (using latest query)
        simple_result = results['simple_feature']
        self.assertIsInstance(simple_result, DependentEntity)
        self.assertEqual(simple_result.computed_value, 2.0)  # 1.0 * 2.0
        
        # Check list_feature result (using range query)
        list_result = results['list_feature']
        self.assertIsInstance(list_result, DependentEntity)
        self.assertEqual(list_result.computed_value, 1.0)  # sum of single test entity value

    def test_multiple_queries(self):
        """Wrapper for async test"""
        asyncio.run(self.async_test_multiple_queries())

    def test_executor_initialization(self):
        """Test executor initialization and feature categorization"""
        # Check pipeline features
        self.assertIn('pipeline_feature', self.executor._pipeline_features)
        
        # Check on-demand features
        self.assertIn('simple_feature', self.executor._ondemand_features)
        self.assertIn('list_feature', self.executor._ondemand_features)
        self.assertIn('chained_feature', self.executor._ondemand_features)
        
        # Check pipeline dependencies mapping
        pipeline_deps = self.executor._pipeline_to_on_demand_query['pipeline_feature']

        self.assertIsInstance(pipeline_deps, dict)
        self.assertEqual(len(pipeline_deps), 2)  # Both simple_feature and list_feature depend on pipeline_feature
        
        # Verify simple_feature dependency
        self.assertIn('simple_feature', pipeline_deps)
        self.assertEqual(pipeline_deps['simple_feature'], 'latest')
        
        # Verify list_feature dependency
        self.assertIn('list_feature', pipeline_deps)
        self.assertEqual(pipeline_deps['list_feature'], 'range')
        
        # Check dependency graph
        expected_graph = {
            'simple_feature': {OnDemandExecutor.get_pipeline_node_name('pipeline_feature', 'simple_feature')},
            'list_feature': {OnDemandExecutor.get_pipeline_node_name('pipeline_feature', 'list_feature')},
            'chained_feature': {'simple_feature'},
            OnDemandExecutor.get_pipeline_node_name('pipeline_feature', 'simple_feature'): set(),
            OnDemandExecutor.get_pipeline_node_name('pipeline_feature', 'list_feature'): set(),
            'pipeline_feature': set(),
        }
        self.assertEqual(self.executor._dependency_graph, expected_graph)

    def test_get_execution_order(self):
        """Test that _get_execution_order returns correct execution levels"""
        
        # Define features with different dependency patterns
        @source(TestEntity)
        def base_feature1() -> Connector:
            return KafkaSource.mock_with_delayed_items(
                items=[test_entity],
                delay_s=0
            )

        @source(TestEntity)
        def base_feature2() -> Connector:
            return KafkaSource.mock_with_delayed_items(
                items=[test_entity],
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
        executor = OnDemandExecutor(MockDataConnector())
        executor.register_features(FeatureRepository.get_all_features())
        # Test different execution order scenarios
        
        # Scenario 1: Single feature with dependencies
        execution_order = executor._get_execution_order(['level2_feature'])
        self.assertEqual(len(execution_order), 3)  # Should have 3 levels
        self.assertEqual(
            set(execution_order[0]), 
            {OnDemandExecutor.get_pipeline_node_name('base_feature1', 'level1_feature1'), OnDemandExecutor.get_pipeline_node_name('base_feature2', 'level1_feature2')}
        )
        self.assertEqual(
            set(execution_order[1]), 
            {'level1_feature1', 'level1_feature2'}
        )
        self.assertEqual(execution_order[2], ['level2_feature'])

        # Scenario 2: Multiple target features at different levels
        execution_order = executor._get_execution_order(['level2_feature', 'level1_feature1'])
        self.assertEqual(len(execution_order), 3)  # Should have 3 levels
        self.assertEqual(
            set(execution_order[0]), 
            {OnDemandExecutor.get_pipeline_node_name('base_feature1', 'level1_feature1'), OnDemandExecutor.get_pipeline_node_name('base_feature2', 'level1_feature2')}
        )
        self.assertEqual(
            set(execution_order[1]), 
            {'level1_feature1', 'level1_feature2'}
        )
        self.assertEqual(execution_order[2], ['level2_feature'])

        # Scenario 3: Full dependency chain
        execution_order = executor._get_execution_order(['level3_feature'])
        self.assertEqual(len(execution_order), 4)  # Should have 4 levels
        self.assertEqual(
            set(execution_order[0]), 
            {OnDemandExecutor.get_pipeline_node_name('base_feature1', 'level1_feature1'), OnDemandExecutor.get_pipeline_node_name('base_feature2', 'level1_feature2')}
        )
        self.assertEqual(
            set(execution_order[1]), 
            {'level1_feature1', 'level1_feature2'}
        )
        self.assertEqual(execution_order[2], ['level2_feature'])
        self.assertEqual(execution_order[3], ['level3_feature'])

        # Scenario 4: Independent features
        execution_order = executor._get_execution_order(['level1_feature1', 'level1_feature2'])
        self.assertEqual(len(execution_order), 2)  # Should have 2 levels
        self.assertEqual(
            set(execution_order[0]), 
            {OnDemandExecutor.get_pipeline_node_name('base_feature1', 'level1_feature1'), OnDemandExecutor.get_pipeline_node_name('base_feature2', 'level1_feature2')}
        )
        self.assertEqual(
            set(execution_order[1]), 
            {'level1_feature1', 'level1_feature2'}
        )

    def test_validate_request(self):
        """Test request validation with various scenarios"""
        # Valid request with keys on on-demand feature that directly uses pipeline
        valid_request = OnDemandRequest(
            target_features=['list_feature'],
            feature_keys={
                'list_feature': {'id': 'test-id'}  # list_feature directly uses pipeline
            },
            query_args={
                'list_feature': {
                    'start_time': datetime.now(),
                    'end_time': datetime.now()
                }
            }
        )
        valid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())

        # Valid request for chained feature (keys only on the feature that uses pipeline)
        valid_chained_request = OnDemandRequest(
            target_features=['chained_feature'],
            feature_keys={
                'simple_feature': {'id': 'test-id'}  # only simple_feature needs keys
            },
            udf_args={
                'simple_feature': {'multiplier': 2.0},
                'chained_feature': {'offset': 1.0}
            }
        )
        valid_chained_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())

        # Test keys on chained feature
        with self.assertRaises(ValueError) as cm:
            invalid_request = OnDemandRequest(
                target_features=['chained_feature'],
                feature_keys={
                    'simple_feature': {'id': 'test-id'},
                    'chained_feature': {'id': 'test-id'}  # chained_feature shouldn't have keys
                }
            )
            invalid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())
        self.assertIn("Keys should not be provided for on-demand feature chained_feature", str(cm.exception))

        # Test missing keys for feature that uses pipeline
        with self.assertRaises(ValueError) as cm:
            invalid_request = OnDemandRequest(
                target_features=['simple_feature'],
                feature_keys={},  # missing keys for simple_feature
                udf_args={'simple_feature': {'multiplier': 2.0}}
            )
            invalid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())
        self.assertIn("Missing keys for on-demand feature simple_feature that directly depends on pipeline features", str(cm.exception))

        # Test missing query args for range query
        with self.assertRaises(ValueError) as cm:
            invalid_request = OnDemandRequest(
                target_features=['list_feature'],
                feature_keys={'list_feature': {'id': 'test-id'}}
            )
            invalid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())
        self.assertIn("Missing required query args for feature list_feature", str(cm.exception))

        # Test query args for feature without pipeline dependencies
        with self.assertRaises(ValueError) as cm:
            invalid_request = OnDemandRequest(
                target_features=['chained_feature'],
                feature_keys={
                    'simple_feature': {'id': 'test-id'}
                },
                query_args={'chained_feature': {'some_arg': 'value'}}
            )
            invalid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())
        self.assertIn("Query args provided for feature chained_feature with no pipeline dependencies requiring parameters", str(cm.exception))

        # Test missing required query parameters
        with self.assertRaises(ValueError) as cm:
            invalid_request = OnDemandRequest(
                target_features=['list_feature'],
                feature_keys={'list_feature': {'id': 'test-id'}},
                query_args={'list_feature': {'start_time': datetime.now()}}  # missing end_time
            )
            invalid_request.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())
        self.assertIn("Missing required query parameters for feature list_feature", str(cm.exception))

        # Test valid request without optional UDF args
        valid_no_udf = OnDemandRequest(
            target_features=['simple_feature'],
            feature_keys={'simple_feature': {'id': 'test-id'}}
        )
        valid_no_udf.validate_request(FeatureRepository.get_all_features(), self.executor._data_connector.query_params())

if __name__ == '__main__':
    unittest.main()
    # t = TestOnDemandExecutor()
    # t.setUp()
    # t.test_validate_request()
    # t.tearDown()
