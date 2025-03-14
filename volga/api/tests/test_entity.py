import datetime
import unittest
from volga.api.entity import entity, field, EntityMetadata, Entity, create_entity
from volga.api.schema import Schema


class TestEntity(unittest.TestCase):
    def test_entity_schema(self):
        @entity
        class User:
            user_id: str = field(key=True)
            timestamp: datetime.datetime = field(timestamp=True)
            name: str

        metadata = User._entity_metadata
        fields = metadata.fields
        key_fields = metadata.key_fields
        timestamp_field = metadata.timestamp_field

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


if __name__ == '__main__':
    unittest.main() 