import datetime
import unittest
from volga.api.entity import entity, field
from volga.api.source import KafkaSource, MysqlSource, source, Connector
from volga.api.feature import FeatureRepository
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream_source import StreamSource

class TestSource(unittest.TestCase):
    def test_source(self):
        FeatureRepository.clear()

        @entity
        class User:
            user_id: str = field(key=True)
            name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @source(User)
        def online_source() -> Connector:
            return KafkaSource.mock_with_delayed_items([User(user_id='123', timestamp=datetime.datetime.now(), name='John')], 0)
        
        @source(User)
        def offline_source() -> Connector:
            return MysqlSource.mock_with_items([User(user_id='123', timestamp=datetime.datetime.now(), name='John')])

        source_pipelines = User._entity._pipelines
        assert len(source_pipelines) == 2
        assert 'online_source' in source_pipelines
        assert 'offline_source' in source_pipelines

        # Verify source features have no dependencies
        online_feature = FeatureRepository.get_feature('online_source')
        assert len(online_feature.dep_args) == 0
        assert online_feature.is_source == True

        offline_feature = FeatureRepository.get_feature('offline_source')
        assert len(offline_feature.dep_args) == 0
        assert offline_feature.is_source == True

        ctx = StreamingContext()
        kafka_pipeline = source_pipelines['online_source']
        kafka_connector = kafka_pipeline.func()
        assert isinstance(kafka_connector, Connector)

        kc = kafka_connector.to_stream_source(ctx)
        assert isinstance(kc, StreamSource)

        mysql_pipeline = source_pipelines['offline_source']
        mysql_connector = mysql_pipeline.func()
        assert isinstance(mysql_connector, Connector)

        mc = mysql_connector.to_stream_source(ctx)
        assert isinstance(mc, StreamSource)

if __name__ == '__main__':
    unittest.main() 