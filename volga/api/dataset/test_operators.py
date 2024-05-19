from datetime import datetime
import unittest

from volga.api.dataset.dataset import dataset, field
from volga.api.dataset.operators import Join


class TestOperator(unittest.TestCase):

    # {'buyer_id': '0', 'product_type': 'ON_SALE', 'purchased_at': '2024-05-07 14:08:26.519626', 'product_price': 100.0, 'name': 'username_0'}
    # {'buyer_id': '0', 'product_id': 'prod_0', 'product_type': 'ON_SALE', 'purchased_at': '2024-05-07 14:14:20.335705', 'product_price': 100.0, 'user_id': '0', 'registered_at': '2024-05-07 14:14:20.335697', 'name': 'username_0'}

    def test_join_schema(self):
        @dataset
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str

        @dataset
        class Order:
            buyer_id: str = field(key=True)
            product_id: str = field(key=True)
            product_type: str
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        ls = User.schema()
        rs = Order.schema()
        joined_schema = Join._joined_schema(ls=ls, rs=rs, on=None, left_on=['user_id'])
        # print(joined_schema)
        assert joined_schema.keys == {'user_id': str}
        assert joined_schema.timestamp == 'registered_at'
        assert joined_schema.values == {
            'name': str,
            'buyer_id': str,
            'product_id': str,
            'product_type': str,
            'product_price': float,
            'purchased_at': datetime
        }


if __name__ == '__main__':
    t = TestOperator()
    t.test_join_schema()
