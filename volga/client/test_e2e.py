import unittest
import datetime

from volga.client.client import Client
from volga.data.api.dataset.aggregate import Avg, Count, Sum
from volga.data.api.dataset.dataset import dataset, field, Dataset
from volga.data.api.dataset.pipeline import pipeline
from volga.data.api.source.source import MysqlSource, source, KafkaSource

# mock data
num_users = 3
num_orders = 10

user_items = [{
    'user_id': str(i),
    'registered_at': str(datetime.datetime.now()),
    'name': f'username_{i}'
} for i in range(num_users)]

order_items = [{
    'buyer_id': str(i % num_users),
    'product_id': f'prod_{i}',
    'product_type': 'ON_SALE' if i % 2 == 0 else 'NORMAL',
    'purchased_at': str(datetime.datetime.now()),
    'product_price': 100.0
} for i in range(num_orders)]


@source(MysqlSource.mock_with_items(user_items))
@dataset
class User:
    user_id: str = field(key=True)
    registered_at: datetime.datetime = field(timestamp=True)
    name: str


@source(MysqlSource.mock_with_items(order_items), tag='offline')
@source(KafkaSource.mock_with_delayed_items(order_items, delay_s=1), tag='online')
@dataset
class Order:
    buyer_id: str = field(key=True)
    product_id: str = field(key=True)
    product_type: str
    purchased_at: datetime.datetime = field(timestamp=True)
    product_price: float


@dataset
class OnSaleUserSpentInfo:
    user_id: str = field(key=True)
    product_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)

    avg_spent_7d: float
    avg_spent_1h: float
    # num_purchases_1w: int

    sum_spent_1h: float

    @pipeline(inputs=[User, Order])
    def gen(cls, users: Dataset, orders: Dataset):
        # on_sale_purchases = orders.filter(lambda df: df['product_type'] == 'ON_SALE')

        per_user = users.join(orders, left_on=['user_id'], right_on=['buyer_id'])

        # return per_user

        return per_user.group_by(keys=['user_id']).aggregate([
            Sum(on='product_price', window='1h', into='sum_spent_1h'),
            Avg(on='product_price', window='7d', into='avg_spent_7d'),
            Avg(on='product_price', window='1h', into='avg_spent_1h'),
            # Count(window='1w', into='num_purchases_1w'),
        ])


class TestVolgaE2E(unittest.TestCase):

    def test_materialize_offline(self):

        client = Client()

        # run batch materialization job
        client.materialize_offline(target=OnSaleUserSpentInfo, source_tags={Order: 'offline'})


if __name__ == '__main__':
    t = TestVolgaE2E()
    t.test_materialize_offline()