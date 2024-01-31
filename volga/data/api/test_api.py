import datetime
import unittest

from volga.data.api.dataset.dataset import dataset, field, Dataset


class TestApi(unittest.TestCase):

    def test_dataset(self):

        @dataset
        class User:
            user_id: str = field(key=True)
            timestamp: datetime.datetime = field(timestamp=True)
            name: str

        assert isinstance(User, Dataset)
        fields = User._fields
        key_fields = User._key_fields
        timestamp_field = User._timestamp_field

        assert len(fields) == 3
        assert key_fields == ['user_id']
        assert timestamp_field == 'timestamp'


if __name__ == '__main__':
    t = TestApi()
    t.test_api()