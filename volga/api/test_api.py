import datetime
import unittest

from volga.api.entity.entity import entity, field, Entity
from volga.api.entity.pipeline import pipeline, PIPELINE_ATTR
from volga.api.source.source import KafkaSource, MysqlSource, source
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream_source import StreamSource


class TestApi(unittest.TestCase):

    def test_entity_schema(self):

        @entity
        class User:
            user_id: str = field(key=True)
            timestamp: datetime.datetime = field(timestamp=True)
            name: str

        fields = User._entity._fields
        key_fields = User._entity._key_fields
        timestamp_field = User._entity._timestamp_field

        assert len(fields) == 3
        assert key_fields == ['user_id']
        assert timestamp_field == 'timestamp'
    
    def test_entity_creation(self):
        @entity
        class User:
            user_id: str = field(key=True)
            timestamp: datetime.datetime = field(timestamp=True)
            name: str = field()
            age: int = 0

        # Test instance creation with parameters
        now = datetime.datetime.now()
        user = User(user_id='123', timestamp=now, name='John', age=30)
        self.assertEqual(user.user_id, '123')
        self.assertEqual(user.timestamp, now)
        self.assertEqual(user.name, 'John')
        self.assertEqual(user.age, 30)

    def test_pipeline(self):
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

        @pipeline(inputs=[User, Order], output=UserOrderInfo)
        def sample_pipeline(cls, users: Entity, orders: Entity):
            p = users.join(orders, on=['user_id'])
            p = p.filter(lambda x: x != '')
            p = p.dropnull()
            return p

        # Check that pipeline is stored both on function and entity
        assert hasattr(sample_pipeline, PIPELINE_ATTR)
        assert getattr(sample_pipeline, PIPELINE_ATTR) == UserOrderInfo._entity._pipelines['sample_pipeline']
        
        # Verify pipeline inputs
        pipe = UserOrderInfo._entity._pipelines['sample_pipeline']
        assert len(pipe.inputs) == 2
        assert pipe.inputs == [User._entity, Order._entity]

    def test_source(self):
        kafka = KafkaSource.mock_with_delayed_items([1], 0)
        mysql = MysqlSource.mock_with_items([1])

        @source(kafka, 'online')
        @source(mysql, 'offline')
        @entity
        class User:
            user_id: str = field(key=True)
            name: str
            timestamp: datetime.datetime = field(timestamp=True)

        sources = User._entity._get_source_connectors()
        assert len(sources) == 2
        assert 'online' in sources
        assert 'offline' in sources

        ctx = StreamingContext()
        kafka_connector = sources['online']
        kc = kafka_connector.to_stream_source(ctx)
        assert isinstance(kc, StreamSource)

        mysql_connector = sources['offline']
        mc = mysql_connector.to_stream_source(ctx)
        assert isinstance(mc, StreamSource)

if __name__ == '__main__':
    unittest.main()
