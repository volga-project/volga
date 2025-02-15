import unittest
import datetime
from typing import Optional

from volga.api.entity.entity import entity, field


class TestEntity(unittest.TestCase):
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