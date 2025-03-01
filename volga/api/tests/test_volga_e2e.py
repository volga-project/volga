import time
import unittest
import datetime

import ray

from volga.api.client import Client
from volga.api.entity.aggregate import Avg, Count, Sum
from volga.api.entity.entity import dataset, field, Entity
from volga.api.entity.pipeline import pipeline
from volga.api.source.source import MysqlSource, source, KafkaSource
from volga.storage.common.in_memory_actor import SimpleInMemoryActorStorage

# mock data
num_users = 2
user_items = [{
    'user_id': str(i),
    'registered_at': str(datetime.datetime.now()),
    'name': f'username_{i}'
} for i in range(num_users)]

num_orders = 10
purchase_time = datetime.datetime.now()
DELAY_S = 60*29
order_items = [{
    'buyer_id': str(i % num_users),
    'product_id': f'prod_{i}',
    'product_type': 'ON_SALE' if i % 2 == 0 else 'NORMAL',
    'purchased_at': str(purchase_time + datetime.timedelta(seconds=i*DELAY_S)),
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
    num_purchases_1h: int
    num_purchases_1d: int
    sum_spent_1h: float
    sum_spent_1d: float

    @pipeline(inputs=[User, Order])
    def gen(cls, users: Entity, orders: Entity):
        on_sale_purchases = orders.filter(lambda df: df['product_type'] == 'ON_SALE')
        per_user = on_sale_purchases.join(users, right_on=['user_id'], left_on=['buyer_id'])

        # return per_user
        return per_user.group_by(keys=['user_id']).aggregate([
            Sum(on='product_price', window='1h', into='sum_spent_1h'),
            Sum(on='product_price', window='1d', into='sum_spent_1d'),
            Avg(on='product_price', window='7d', into='avg_spent_7d'),
            Avg(on='product_price', window='1h', into='avg_spent_1h'),
            Count(window='1h', into='num_purchases_1h'),
            Count(window='1d', into='num_purchases_1d'),
        ])


class TestVolgaE2E(unittest.TestCase):

    def test_materialize_offline(self):
        storage = SimpleInMemoryActorStorage()
        client = Client(cold=storage)
        # run batch materialization job
        ray.init(address='auto', ignore_reinit_error=True)
        client.materialize_offline(
            target=OnSaleUserSpentInfo,
            source_tags={Order: 'offline'}
        )
        time.sleep(1)
        res = client.get_offline_data(dataset_name=OnSaleUserSpentInfo.__name__, keys=[{'user_id': 0}], start=None, end=None)
        print(res)
        # TODO assert expected
        ray.shutdown()

    def test_materialize_online(self):
        storage = SimpleInMemoryActorStorage()
        client = Client(hot=storage)
        # run online materialization job
        ray.init(address='auto', ignore_reinit_error=True)
        client.materialize_online(
            target=OnSaleUserSpentInfo,
            source_tags={Order: 'online'},
            _async=True
        )
        time.sleep(1)
        live_on_sale_user_spent = None
        while True:
            res = client.get_online_latest_data(dataset_name=OnSaleUserSpentInfo.__name__, keys=[{'user_id': 0}])
            if live_on_sale_user_spent == res:
                continue
            live_on_sale_user_spent = res
            print(f'[{time.time()}]{res}')
        # TODO store results and assert expected
        ray.shutdown()


if __name__ == '__main__':
    t = TestVolgaE2E()
    t.test_materialize_offline()

    # uncomment for online case
    # t.test_materialize_online()
