import time
import unittest
import datetime
import ray
import asyncio

from volga.api.aggregate import Avg, Count
from volga.api.entity import entity, field
from volga.api.pipeline import pipeline
from volga.api.source import source, KafkaSource, MysqlSource
from volga.api.on_demand import on_demand
from volga.client.client import Client
from volga.api.storage import InMemoryActorPipelineDataConnector
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga.on_demand.client import OnDemandClient
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.models import OnDemandRequest, OnDemandArgs
from volga.on_demand.config import DEFAULT_ON_DEMAND_CONFIG


# Mock data
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

@entity
class User:
    user_id: str = field(key=True)
    registered_at: datetime.datetime = field(timestamp=True)
    name: str

@entity
class Order:
    buyer_id: str = field(key=True)
    product_id: str = field(key=True)
    product_type: str
    purchased_at: datetime.datetime = field(timestamp=True)
    product_price: float

@entity
class OnSaleUserSpentInfo:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    avg_spent_7d: float
    num_purchases_1h: int

@source(User)
def user_source():
    return MysqlSource.mock_with_items(user_items)

@source(Order)
def order_source():
    return KafkaSource.mock_with_delayed_items(order_items, delay_s=1)

@pipeline(dependencies=['user_source', 'order_source'], output=OnSaleUserSpentInfo)
def user_spent_pipeline(users: User, orders: Order):
    on_sale_purchases = orders.filter(lambda df: df['product_type'] == 'ON_SALE')
    per_user = on_sale_purchases.join(users, right_on=['user_id'], left_on=['buyer_id'])
    return per_user.group_by(keys=['user_id']).aggregate([
        Avg(on='product_price', window='7d', into='avg_spent_7d'),
        Count(window='1h', into='num_purchases_1h'),
    ])

@entity
class UserStats:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    total_spent: float
    purchase_count: int

@on_demand(dependencies=['user_spent_pipeline'])
def user_stats(spent_info: OnSaleUserSpentInfo) -> UserStats:
    return UserStats(
        user_id=spent_info.user_id,
        timestamp=spent_info.timestamp,
        total_spent=spent_info.avg_spent_7d * spent_info.num_purchases_1h,
        purchase_count=spent_info.num_purchases_1h
    )

class TestVolgaE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    async def test_e2e(self):
        # Initialize connectors
        pipeline_connector = InMemoryActorPipelineDataConnector(batch=False)
        on_demand_connector = InMemoryActorOnDemandDataConnector()
        await on_demand_connector.init()

        # Initialize client and start materialization
        client = Client()
        client.materialize(
            features=['user_spent_pipeline'],
            pipeline_data_connector=pipeline_connector,
            _async=True
        )

        # Start OnDemand coordinator
        coordinator = create_on_demand_coordinator(DEFAULT_ON_DEMAND_CONFIG)
        servers_per_node = ray.get(coordinator.start.remote())
        
        # Wait for data to be materialized
        time.sleep(5)

        # Create OnDemand client and request
        on_demand_client = OnDemandClient(DEFAULT_ON_DEMAND_CONFIG)
        request = OnDemandRequest(args=[
            OnDemandArgs(
                feature_name='user_stats',
                serve_or_udf=False,
                dep_features_keys=[('user_spent_pipeline', {'user_id': '0'})]
            )
        ])

        # Make request and validate response
        response = await on_demand_client.request(request)
        stats = response.feature_values['user_stats']
        
        self.assertEqual(stats.keys['user_id'], '0')
        self.assertIsInstance(stats.values['total_spent'], float)
        self.assertIsInstance(stats.values['purchase_count'], int)
        self.assertTrue(stats.values['purchase_count'] >= 0)
        self.assertTrue(stats.values['total_spent'] >= 0)

if __name__ == '__main__':
    unittest.main()