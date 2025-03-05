import time
import unittest
import datetime
import ray
import asyncio
import queue
from typing import Dict, List
from queue import Queue
import threading

from volga.api.aggregate import Avg, Count
from volga.api.entity import Entity, entity, field
from volga.api.feature import FeatureRepository
from volga.api.pipeline import pipeline
from volga.api.source import Connector, source, KafkaSource, MysqlSource
from volga.api.on_demand import on_demand
from volga.client.client import Client
from volga.api.storage import InMemoryActorPipelineDataConnector
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga.on_demand.client import OnDemandClient
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.models import OnDemandRequest
from volga.on_demand.config import DEFAULT_ON_DEMAND_CONFIG

# Mock data
num_users = 2
user_items = [{
    'user_id': str(i),
    'registered_at': datetime.datetime.now(),
    'name': f'username_{i}'
} for i in range(num_users)]

num_orders = 10
purchase_time = datetime.datetime.now()
DELAY_S = 60*29
order_items = [{
    'buyer_id': str(i % num_users),
    'product_id': f'prod_{i}',
    'product_type': 'ON_SALE' if i % 2 == 0 else 'NORMAL',
    'purchased_at': purchase_time + datetime.timedelta(seconds=i*DELAY_S),
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

@entity
class UserStats:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    total_spent: float
    purchase_count: int

@source(User)
def user_source() -> Connector:
    return MysqlSource.mock_with_items(user_items)

@source(Order)
def order_source() -> Connector:
    return KafkaSource.mock_with_delayed_items(order_items, delay_s=1)

@pipeline(dependencies=['user_source', 'order_source'], output=OnSaleUserSpentInfo)
def user_spent_pipeline(users: Entity, orders: Entity) -> Entity:
    on_sale_purchases = orders.filter(lambda x: x['product_type'] == 'ON_SALE')
    per_user = on_sale_purchases.join(
        users, 
        left_on=['buyer_id'], 
        right_on=['user_id'],
        how='left'
    )
    return per_user.group_by(keys=['buyer_id']).aggregate([
        Avg(on='product_price', window='7d', into='avg_spent_7d'),
        Count(window='1h', into='num_purchases_1h'),
    ]).rename(columns={
        'purchased_at': 'timestamp',
        'buyer_id': 'user_id'
    })

@on_demand(dependencies=['user_spent_pipeline'])
def user_stats(spent_info: OnSaleUserSpentInfo) -> UserStats:
    return UserStats(
        user_id=spent_info.user_id,
        timestamp=spent_info.timestamp,
        total_spent=spent_info.avg_spent_7d * spent_info.num_purchases_1h,
        purchase_count=spent_info.num_purchases_1h
    )

class OnDemandClientThread(threading.Thread):
    def __init__(self, user_ids: List[str], results_queues: Dict[str, Queue]):
        super().__init__()
        self.user_ids = user_ids
        self.results_queues = results_queues
        self.stop_event = threading.Event()
        self.client = OnDemandClient(DEFAULT_ON_DEMAND_CONFIG)
        self.last_timestamps: Dict[str, str] = {}  # Track last timestamp per user
        
    def run(self):
        async def make_requests():
            while not self.stop_event.is_set():
                try:
                    request = OnDemandRequest(
                        target_features=['user_stats'],
                        feature_keys={
                            'user_spent_pipeline': [
                                {'user_id': user_id} 
                                for user_id in self.user_ids
                            ]
                        }
                    )
                    
                    response = await self.client.request(request)
                    stats_list = response.feature_values['user_stats']
                    
                    # stats_list order corresponds to user_ids order
                    for user_id, user_stats in zip(self.user_ids, stats_list):
                        # Check if timestamp has changed before putting in queue
                        current_timestamp = user_stats[0]['timestamp']
                        if current_timestamp != self.last_timestamps.get(user_id):
                            self.last_timestamps[user_id] = current_timestamp
                            self.results_queues[user_id].put(user_stats)
                            
                except Exception as e:
                    for user_id in self.user_ids:
                        self.results_queues[user_id].put(e)
                        
                await asyncio.sleep(1)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(make_requests())
        
    def stop(self):
        self.stop_event.set()

class TestVolgaE2E(unittest.IsolatedAsyncioTestCase):
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

        # Start OnDemand coordinator
        coordinator = create_on_demand_coordinator(DEFAULT_ON_DEMAND_CONFIG)
        ray.get(coordinator.start.remote())

        # Create results queues and start client thread
        user_ids = [str(i) for i in range(num_users)]
        results_queues = {user_id: Queue() for user_id in user_ids}
        client_thread = OnDemandClientThread(user_ids, results_queues)
        client_thread.start()

        try:
            # Initialize client and start materialization
            client = Client()
            client.materialize(
                features=[FeatureRepository.get_feature('user_spent_pipeline')],
                pipeline_data_connector=pipeline_connector,
                _async=True
            )

            # Track stats over time
            user_stats_history: Dict[str, List[UserStats]] = {
                user_id: [] for user_id in user_ids
            }
            
            # Monitor results for 15 seconds
            end_time = time.time() + 3
            while time.time() < end_time:
                # Sleep briefly to avoid tight loop
                await asyncio.sleep(0.1)
                
                # Check all queues without blocking
                for user_id in user_ids:
                    try:
                        result = results_queues[user_id].get_nowait()
                    except queue.Empty:
                        continue
                        
                    if isinstance(result, Exception):
                        # print(f"Error for user {user_id}: {result}")
                        continue
                        
                    # Convert raw stats to UserStats object
                    stats = UserStats(
                        user_id=result[0]['keys']['user_id'],
                        timestamp=datetime.datetime.fromisoformat(result[0]['timestamp']),
                        total_spent=result[0]['values']['total_spent'],
                        purchase_count=result[0]['values']['purchase_count']
                    )
                    
                    # Validate user_id matches
                    self.assertEqual(stats.user_id, user_id)
                    
                    user_stats_history[user_id].append(stats)
                    
                    # Validate stats using entity fields
                    self.assertIsInstance(stats.total_spent, float)
                    self.assertIsInstance(stats.purchase_count, int)
                    self.assertTrue(stats.purchase_count >= 0)
                    self.assertTrue(stats.total_spent >= 0)
                    
                    # Validate stats are monotonically increasing
                    if len(user_stats_history[user_id]) > 1:
                        prev_stats = user_stats_history[user_id][-2]
                        self.assertTrue(
                            stats.purchase_count >= prev_stats.purchase_count,
                            f"Purchase count decreased for user {user_id}"
                        )
                        self.assertTrue(
                            stats.total_spent >= prev_stats.total_spent,
                            f"Total spent decreased for user {user_id}"
                        )

            # Validate final stats
            for user_id in user_ids:
                history = user_stats_history[user_id]
                self.assertTrue(len(history) > 0, f"No stats received for user {user_id}")
                final_stats = history[-1]
                expected_count = sum(1 for order in order_items 
                                   if order['buyer_id'] == user_id 
                                   and order['product_type'] == 'ON_SALE')
                self.assertTrue(
                    final_stats.purchase_count <= expected_count,
                    f"Purchase count {final_stats.purchase_count} exceeds expected {expected_count} for user {user_id}"
                )

        finally:
            # Cleanup
            client_thread.stop()
            client_thread.join()
            await on_demand_connector.close()

if __name__ == '__main__':
    unittest.main()