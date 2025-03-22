import datetime
import unittest
from volga.api.entity import entity, field, Entity
from volga.api.pipeline import pipeline, PipelineFeature
from volga.api.feature import FeatureRepository, DepArg
from volga.api.source import MockOnlineConnector, MockOfflineConnector, source, Connector

class TestPipeline(unittest.TestCase):
    def test_sample_pipeline_creation(self):
        FeatureRepository.clear()

        @entity
        class User:
            user_id: str = field(key=True)
            name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @entity
        class Order:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            product_name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @entity
        class UserOrderInfo:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            product_name: str
            user_name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @source(User)
        def online_source() -> Connector:
            return MockOnlineConnector.with_periodic_items([User(user_id='123', timestamp=datetime.datetime.now(), name='John')], 0)
        
        @source(Order)
        def offline_source() -> Connector:
            return MockOfflineConnector.with_items([Order(user_id='123', product_id='123', product_name='Product', timestamp=datetime.datetime.now())])

        @pipeline(dependencies=['online_source', 'offline_source'], output=UserOrderInfo)
        def sample_pipeline(users: Entity, orders: Entity):
            p = users.join(orders, on=['user_id'])
            p = p.filter(lambda x: x != '')
            p = p.dropnull()
            return p

        # Verify pipelines exist on entities
        assert 'sample_pipeline' in UserOrderInfo._entity_metadata.get_pipeline_features()
        assert isinstance(UserOrderInfo._entity_metadata.get_pipeline_features()['sample_pipeline'], PipelineFeature)

        assert 'online_source' in User._entity_metadata.get_pipeline_features()
        assert isinstance(User._entity_metadata.get_pipeline_features()['online_source'], PipelineFeature)

        assert 'offline_source' in Order._entity_metadata.get_pipeline_features()
        assert isinstance(Order._entity_metadata.get_pipeline_features()['offline_source'], PipelineFeature)

        # Check online source
        online_feature = FeatureRepository.get_feature('online_source')
        assert online_feature is not None
        assert online_feature.name == 'online_source'
        assert online_feature.output_type == User
        assert len(online_feature.dep_args) == 0
        assert online_feature.is_source == True

        # Check offline source 
        offline_feature = FeatureRepository.get_feature('offline_source')
        assert offline_feature is not None
        assert offline_feature.name == 'offline_source'
        assert offline_feature.output_type == Order
        assert len(offline_feature.dep_args) == 0
        assert offline_feature.is_source == True

        # Check pipeline
        pipeline_feature = FeatureRepository.get_feature('sample_pipeline')
        assert pipeline_feature is not None
        assert pipeline_feature.name == 'sample_pipeline'
        assert pipeline_feature.output_type == UserOrderInfo
        assert len(pipeline_feature.dep_args) == 2
        assert any(dep.get_name() == 'online_source' for dep in pipeline_feature.dep_args)
        assert any(dep.get_name() == 'offline_source' for dep in pipeline_feature.dep_args)
        assert pipeline_feature.is_source == False

if __name__ == '__main__':
    unittest.main() 