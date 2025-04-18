import datetime
import enum
from pprint import pprint
import time

import ray

from volga.api.feature import FeatureRepository
from volga.api.on_demand import on_demand
from volga.common.ray.ray_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.config import OnDemandConfig, OnDemandDataConnectorConfig
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga.on_demand.storage.redis import OnDemandRedisConnector
from volga.storage.common.in_memory_actor import get_or_create_in_memory_cache_actor
from volga_tests.on_demand_perf.load_test_handler import LoadTestHandler
from volga.on_demand.testing_utils import TEST_FEATURE_NAME, TestEntity, setup_sample_in_memory_actor_feature_data_ray, setup_sample_redis_feature_data_ray, setup_sample_scylla_feature_data_ray
from volga.on_demand.storage.scylla import OnDemandScyllaConnector

class MemoryBackend(enum.Enum):
    IN_MEMORY = 'in_memory'
    SCYLLA = 'scylla'
    REDIS = 'redis'

STORE_DIR = 'volga_on_demand_perf_benchmarks'


RUN_TIME_S = 185
STEP_TIME_S = 10
RPS_PER_USER = 10

NUM_WORKERS = 4

MAX_RPS = 1000 * NUM_WORKERS

MAX_USERS = int(MAX_RPS/RPS_PER_USER)
NUM_STEPS = int(RUN_TIME_S/STEP_TIME_S)
STEP_USER_COUNT = int(MAX_USERS/NUM_STEPS)
MEMORY_BACKEND = MemoryBackend.REDIS
NUM_KEYS = MAX_RPS

# DO NOT FORGET http:// prefix and / at the end
HOST = 'http://k8s-raysyste-volgaond-3637bbe071-1840438529.ap-northeast-1.elb.amazonaws.com/'

SCYLLA_CONTACT_POINTS = ['scylla-client.scylla-operator.svc.cluster.local']

REDIS_HOST = 'redis-master.redis.svc.cluster.local'
REDIS_PASSWORD = 'password'

@on_demand(dependencies=[TEST_FEATURE_NAME])
def simple_feature(
    dep: TestEntity,
    multiplier: float = 1.0
) -> TestEntity:
    """Simple on-demand feature that multiplies the value"""
    return TestEntity(
        id=dep.id,
        value=dep.value * multiplier,
        timestamp=datetime.datetime.now()
    )


run_id = f'{int(time.time())}-{MAX_RPS}-{MEMORY_BACKEND.value}-{NUM_WORKERS}'

print(f'[run-{run_id}] Started On-Demand benchmark')

ray.init(address=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, namespace='default')
# ray.init(namespace='default')

# setup data
if MEMORY_BACKEND == MemoryBackend.IN_MEMORY:
    ray.get(setup_sample_in_memory_actor_feature_data_ray.remote(NUM_KEYS))
    data_connector_config = data_connector=OnDemandDataConnectorConfig(
        connector_class=InMemoryActorOnDemandDataConnector,
        connector_args={}
    )
elif MEMORY_BACKEND == MemoryBackend.SCYLLA:
    ray.get(setup_sample_scylla_feature_data_ray.remote(SCYLLA_CONTACT_POINTS, NUM_KEYS))
    data_connector_config = data_connector=OnDemandDataConnectorConfig(
        connector_class=OnDemandScyllaConnector,
        connector_args={'contact_points': SCYLLA_CONTACT_POINTS}
    )
elif MEMORY_BACKEND == MemoryBackend.REDIS:
    ray.get(setup_sample_redis_feature_data_ray.remote(REDIS_HOST, 6379, 0, REDIS_PASSWORD, NUM_KEYS))
    data_connector_config = data_connector=OnDemandDataConnectorConfig(
        connector_class=OnDemandRedisConnector,
        connector_args={'host': REDIS_HOST, 'port': 6379, 'password': REDIS_PASSWORD}
    )
else:
    raise ValueError(f'Invalid memory backend: {MEMORY_BACKEND}')

on_demand_config = OnDemandConfig(
    num_servers_per_node=2,
    server_port=1122,
    data_connector=data_connector_config
)

coordinator = create_on_demand_coordinator(on_demand_config)
ray.get(coordinator.start.remote())
features = FeatureRepository.get_all_features()
ray.get(coordinator.register_features.remote(features))

# time.sleep(10000)

stats_store_path = f'{STORE_DIR}/run-{run_id}.json'

run_metadata = {
    'run_id': run_id,
    'start_time': str(datetime.datetime.now()),
    'run_time_s': RUN_TIME_S,
    'step_time_s': STEP_TIME_S,
    'step_user_count': STEP_USER_COUNT,
    'max_rps': MAX_RPS,
    'max_users': MAX_USERS,
    'memory_backend': MEMORY_BACKEND.value,
    'num_steps': NUM_STEPS,
    'rps_per_user': RPS_PER_USER,
    'num_keys': NUM_KEYS,
    'num_workers': NUM_WORKERS
}
load_test_handler = LoadTestHandler(stats_store_path, coordinator, run_metadata)

load_test_handler.start_load_test(
    host=HOST,
    step_user_count=STEP_USER_COUNT,
    step_time_s=STEP_TIME_S,
    run_time_s=RUN_TIME_S
)
print(f'[run-{run_id}] Started Locust')

time.sleep(RUN_TIME_S)
load_test_handler.stop()
print(f'[run-{run_id}] Finished On-Demand benchmark')
