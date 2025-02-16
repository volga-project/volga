import datetime
import unittest
from volga.api.entity import entity, field, Entity
from volga.api.pipeline import pipeline
from volga.api.on_demand import on_demand
from volga.api.feature import FeatureRepository
from volga.api.source import KafkaSource, MysqlSource, source, Connector

class TestOnDemand(unittest.TestCase):
    def test_on_demand(self):
        FeatureRepository.clear()
        
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

        @source(User)
        def user_source() -> Connector:
            return KafkaSource.mock_with_delayed_items([User(user_id='123', timestamp=datetime.datetime.now(), name='John')], 0)
        
        @source(Order)
        def order_source() -> Connector:
            return MysqlSource.mock_with_items([Order(user_id='123', product_id='123', product_name='Product', timestamp=datetime.datetime.now())])

        @pipeline(dependencies=['user_source', 'order_source'], output=UserOrderInfo)
        def user_order_pipeline(users: Entity, orders: Entity):
            p = users.join(orders, on=['user_id'])
            p = p.filter(lambda x: x != '')
            p = p.dropnull()
            return p
        
        @entity
        class UserOrderStats:
            user_id: str = field(key=True)
            order_name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @on_demand(dependencies=['user_order_pipeline', 'user_source'])
        def user_order_stats(orders: UserOrderInfo, user: User) -> UserOrderStats:
            return UserOrderStats(user_id=user.user_id, order_name=orders.product_name, timestamp=user.timestamp)  

        # Verify features exist in repository
        assert 'user_source' in FeatureRepository.get_all_features()
        assert 'order_source' in FeatureRepository.get_all_features()
        assert 'user_order_pipeline' in FeatureRepository.get_all_features()
        assert 'user_order_stats' in FeatureRepository.get_all_features()

        # Check user source
        user_source_feature = FeatureRepository.get_feature('user_source')
        assert user_source_feature is not None
        assert user_source_feature.name == 'user_source'
        assert user_source_feature.output_type == User
        assert len(user_source_feature.dependencies) == 0

        # Check order source
        order_source_feature = FeatureRepository.get_feature('order_source')
        assert order_source_feature is not None
        assert order_source_feature.name == 'order_source'
        assert order_source_feature.output_type == Order
        assert len(order_source_feature.dependencies) == 0

        # Check pipeline
        pipeline_feature = FeatureRepository.get_feature('user_order_pipeline')
        assert pipeline_feature is not None
        assert pipeline_feature.name == 'user_order_pipeline'
        assert pipeline_feature.output_type == UserOrderInfo
        assert len(pipeline_feature.dependencies) == 2
        assert 'user_source' in pipeline_feature._dependency_names
        assert 'order_source' in pipeline_feature._dependency_names

        # Check on_demand feature
        on_demand_feature = FeatureRepository.get_feature('user_order_stats')
        assert on_demand_feature is not None
        assert on_demand_feature.name == 'user_order_stats'
        assert on_demand_feature.output_type == UserOrderStats
        assert len(on_demand_feature.dependencies) == 2
        assert 'user_order_pipeline' in on_demand_feature._dependency_names
        assert 'user_source' in on_demand_feature._dependency_names

        assert UserOrderStats._entity._on_demands['user_order_stats'] == on_demand_feature

if __name__ == '__main__':
    unittest.main() 