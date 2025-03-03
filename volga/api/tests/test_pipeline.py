import datetime
import unittest
from volga.api.entity import entity, field, Entity
from volga.api.pipeline import pipeline, PipelineFeature
from volga.api.feature import FeatureRepository, DepArg
from volga.api.source import KafkaSource, MysqlSource, source, Connector

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
        def kafka_source() -> Connector:
            return KafkaSource.mock_with_delayed_items([User(user_id='123', timestamp=datetime.datetime.now(), name='John')], 0)
        
        @source(Order)
        def mysql_source() -> Connector:
            return MysqlSource.mock_with_items([Order(user_id='123', product_id='123', product_name='Product', timestamp=datetime.datetime.now())])

        @pipeline(dependencies=['kafka_source', 'mysql_source'], output=UserOrderInfo)
        def sample_pipeline(users: Entity, orders: Entity):
            p = users.join(orders, on=['user_id'])
            p = p.filter(lambda x: x != '')
            p = p.dropnull()
            return p

        # Verify pipelines exist on entities
        assert 'sample_pipeline' in UserOrderInfo._entity_metadata.get_pipeline_features()
        assert isinstance(UserOrderInfo._entity_metadata.get_pipeline_features()['sample_pipeline'], PipelineFeature)

        assert 'kafka_source' in User._entity_metadata.get_pipeline_features()
        assert isinstance(User._entity_metadata.get_pipeline_features()['kafka_source'], PipelineFeature)

        assert 'mysql_source' in Order._entity_metadata.get_pipeline_features()
        assert isinstance(Order._entity_metadata.get_pipeline_features()['mysql_source'], PipelineFeature)

        # Check kafka source
        kafka_feature = FeatureRepository.get_feature('kafka_source')
        assert kafka_feature is not None
        assert kafka_feature.name == 'kafka_source'
        assert kafka_feature.output_type == User
        assert len(kafka_feature.dep_args) == 0
        assert kafka_feature.is_source == True

        # Check mysql source 
        mysql_feature = FeatureRepository.get_feature('mysql_source')
        assert mysql_feature is not None
        assert mysql_feature.name == 'mysql_source'
        assert mysql_feature.output_type == Order
        assert len(mysql_feature.dep_args) == 0
        assert mysql_feature.is_source == True

        # Check pipeline
        pipeline_feature = FeatureRepository.get_feature('sample_pipeline')
        assert pipeline_feature is not None
        assert pipeline_feature.name == 'sample_pipeline'
        assert pipeline_feature.output_type == UserOrderInfo
        assert len(pipeline_feature.dep_args) == 2
        assert any(dep.get_name() == 'kafka_source' for dep in pipeline_feature.dep_args)
        assert any(dep.get_name() == 'mysql_source' for dep in pipeline_feature.dep_args)
        assert pipeline_feature.is_source == False

if __name__ == '__main__':
    unittest.main() 