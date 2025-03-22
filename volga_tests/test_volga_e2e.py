from pprint import pprint
import time
import unittest
import datetime
import pandas as pd
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
from volga.api.source import Connector, MockOnlineConnector, source, MockOfflineConnector
from volga.api.on_demand import on_demand
from volga.client.client import Client
from volga.api.storage import InMemoryActorPipelineDataConnector
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga.on_demand.client import OnDemandClient
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.models import OnDemandRequest
from volga.on_demand.config import DEFAULT_ON_DEMAND_CLIENT_URL, DEFAULT_ON_DEMAND_CONFIG, DEFAULT_ON_DEMAND_SERVER_PORT, OnDemandConfig, OnDemandDataConnectorConfig
from volga.storage.common.in_memory_actor import delete_in_memory_cache_actor, get_or_create_in_memory_cache_actor

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

# Mock data
num_users = 100
users = [
    User(
        user_id=str(i), 
        registered_at=datetime.datetime.now(), 
        name=f'username_{i}'
    ) for i in range(num_users)
]

num_orders = 1000
purchase_time_base = datetime.datetime.now()
purchase_delay_s = 1 # this corresponds to purchased_at timestamp showed on order item

# TODO this assumes we have no batching enabled for data writer (batch_size=1, see network_config.py)
purchase_event_delays_s = 0.01 # this is how often we actually push event. In real case they would match, for testing they are different

client_loop_sleep_s = purchase_event_delays_s/2

assert num_users <= num_orders, 'num_users must be less than or equal to num_orders'
orders = [
    Order(
        buyer_id=str(i % num_users), 
        product_id=f'prod_{i}', 
        product_type='ON_SALE' if i <= num_orders * 0.8 else 'NORMAL', 
        purchased_at=purchase_time_base + datetime.timedelta(seconds=i*purchase_delay_s), 
        product_price=100.0
    ) for i in range(num_orders)
]

run_time_s = num_orders * purchase_event_delays_s

@source(User)
def user_source() -> Connector:
    return MockOfflineConnector.with_items([user.__dict__ for user in users])

@source(Order)
def order_source(online: bool = True) -> Connector:
    if online:
        return MockOnlineConnector.with_periodic_items([order.__dict__ for order in orders], period_s=purchase_event_delays_s)
    else:
        return MockOfflineConnector.with_items([order.__dict__ for order in orders])

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

@on_demand(dependencies=[('user_spent_pipeline', 'latest')])
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
        self.client = OnDemandClient(DEFAULT_ON_DEMAND_CLIENT_URL)
        self.last_timestamps: Dict[str, str] = {}  # Track last timestamp per user
        
    def run(self):
        async def make_requests():
            while not self.stop_event.is_set():
                try:
                    request = OnDemandRequest(
                        target_features=['user_stats'],
                        feature_keys={
                            'user_stats': [
                                {'user_id': user_id} 
                                for user_id in self.user_ids
                            ]
                        }
                    )
                    
                    response = await self.client.request(request)

                    stats_list = response.results['user_stats']
                    
                    # stats_list order corresponds to user_ids order
                    for user_id, user_stats_raw in zip(self.user_ids, stats_list):
                        if user_stats_raw is None:
                            continue

                        # cast to UserStats
                        user_stats = UserStats(**user_stats_raw[0])

                        # Check if timestamp has changed before putting in queue
                        current_timestamp = user_stats.timestamp
                        if current_timestamp != self.last_timestamps.get(user_id):
                            self.last_timestamps[user_id] = current_timestamp
                            self.results_queues[user_id].put(user_stats)
                            pprint(f'New feature: {user_stats.__dict__}')
                            
                except Exception as e:
                    print(f'Error: {e}')
                    for user_id in self.user_ids:
                        self.results_queues[user_id].put(e)
                        
                await asyncio.sleep(client_loop_sleep_s)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(make_requests())
        
    def stop(self):
        self.stop_event.set()

class TestVolgaE2E(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(ignore_reinit_error=True)
        delete_in_memory_cache_actor()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    async def test_e2e_online(self):
        # Start OnDemand coordinator
        coordinator = create_on_demand_coordinator(OnDemandConfig(
            num_servers_per_node=1,
            server_port=DEFAULT_ON_DEMAND_SERVER_PORT,
            data_connector=OnDemandDataConnectorConfig(
                connector_class=InMemoryActorOnDemandDataConnector,
                connector_args={}
            )
        ))
        await coordinator.start.remote()

        features = FeatureRepository.get_all_features()

        await coordinator.register_features.remote(features)

        # Create results queues and start client thread
        user_ids = [user.user_id for user in users]
        results_queues = {user_id: Queue() for user_id in user_ids}
        client_thread = OnDemandClientThread(user_ids, results_queues)
        client_thread.start()

        try:
            # Initialize client and start online materialization
            client = Client()
            print('Starting online materialization')
            client.materialize(
                features=[FeatureRepository.get_feature('user_spent_pipeline')],
                pipeline_data_connector=InMemoryActorPipelineDataConnector(batch=False),
                _async=True,
                params={'global': {'online': True}}
            )
            

            # Track stats over time
            user_stats_history: Dict[str, List[UserStats]] = {
                user_id: [] for user_id in user_ids
            }
            
            # Monitor results
            end_time = time.time() + run_time_s + 1
            while time.time() < end_time:
                # Sleep briefly to avoid tight loop
                await asyncio.sleep(client_loop_sleep_s)
                
                # Check all queues without blocking
                for user_id in user_ids:
                    try:
                        stats = results_queues[user_id].get_nowait()
                    except queue.Empty:
                        continue
                        
                    if isinstance(stats, Exception):
                        print(f"Error for user {user_id}: {stats}")
                        continue
                    
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
                expected_count = sum(1 for order in orders 
                                   if order.buyer_id == user_id 
                                   and order.product_type == 'ON_SALE')
                expected_total_spent = sum(order.product_price for order in orders 
                                   if order.buyer_id == user_id 
                                   and order.product_type == 'ON_SALE')
                
                # print(f'Expected count: {expected_count}, final_stats.purchase_count: {final_stats.purchase_count}')
                # print(f'Expected total spent: {expected_total_spent}, final_stats.total_spent: {final_stats.total_spent}')

                self.assertTrue(
                    final_stats.purchase_count == expected_count,
                    f"Purchase count {final_stats.purchase_count} does not match expected {expected_count} for user {user_id}"
                )
                self.assertTrue(
                    final_stats.total_spent == expected_total_spent,
                    f"Total spent {final_stats.total_spent} does not match expected {expected_total_spent} for user {user_id}"
                )
        finally:
            client_thread.stop()
            client_thread.join()

    
    async def test_e2e_offline(self):
        print('Starting offline materialization')
        cache_actor = get_or_create_in_memory_cache_actor()
        client = Client()
        client.materialize(
            features=[FeatureRepository.get_feature('user_spent_pipeline')],
            pipeline_data_connector=InMemoryActorPipelineDataConnector(batch=False),
            _async=False,
            params={'global': {'online': False}}
        )   
    
        keys = [{'user_id': user.user_id} for user in users]
        offline_res_raw = ray.get(cache_actor.get_range.remote(feature_name='user_spent_pipeline', keys=keys, start=None, end=None, with_timestamps=False))
        offline_res_flattened = [item for items in offline_res_raw for item in items]
        offline_res_flattened.sort(key=lambda x: x['timestamp'])
        offline_df = pd.DataFrame(offline_res_flattened)
        pprint(offline_df)
        assert len(offline_df) > 0 # TODO proper assert contents
        await asyncio.sleep(1) # for print to work

if __name__ == '__main__':
    unittest.main()