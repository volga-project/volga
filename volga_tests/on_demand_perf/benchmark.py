import datetime
import enum
import time

import ray

from volga.api.feature import FeatureRepository
from volga.api.on_demand import on_demand
from volga.common.ray.ray_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.config import OnDemandConfig, OnDemandDataConnectorConfig
from volga.on_demand.storage.in_memory import InMemoryActorOnDemandDataConnector
from volga_tests.on_demand_perf.load_test_handler import LoadTestHandler
from volga.on_demand.testing_utils import TEST_FEATURE_NAME, TestEntity, setup_sample_in_memory_actor_feature_data_ray, setup_sample_scylla_feature_data_ray
from volga.on_demand.storage.scylla import OnDemandScyllaConnector

class MemoryBackend(enum.Enum):
    IN_MEMORY = 'in_memory'
    SCYLLA = 'scylla'

STORE_DIR = 'volga_on_demand_perf_benchmarks'

run_id = int(time.time())

RUN_TIME_S = 125
STEP_TIME_S = 30
RPS_PER_USER = 10
MAX_RPS = 1000
MAX_USERS = int(MAX_RPS/RPS_PER_USER)
NUM_STEPS = int(RUN_TIME_S/STEP_TIME_S)
STEP_USER_COUNT = int(MAX_USERS/NUM_STEPS)
MEMORY_BACKEND = MemoryBackend.IN_MEMORY

HOST = 'k8s-raysyste-volgaond-3637bbe071-1840438529.ap-northeast-1.elb.amazonaws.com'
SCYLLA_CONTACT_POINTS = ['scylla-client.scylla-operator.svc.cluster.local']

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

print(f'[run-{run_id}] Started On-Demand benchmark')

ray.init(address=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV)
# ray.init()
# setup data
if MEMORY_BACKEND == MemoryBackend.IN_MEMORY:
    ray.get(setup_sample_in_memory_actor_feature_data_ray.remote(1000000))
    data_connector_config = data_connector=OnDemandDataConnectorConfig(
        connector_class=InMemoryActorOnDemandDataConnector,
        connector_args={}
    )
elif MEMORY_BACKEND == MemoryBackend.SCYLLA:
    ray.get(setup_sample_scylla_feature_data_ray.remote(SCYLLA_CONTACT_POINTS, 10000))
    data_connector_config = data_connector=OnDemandDataConnectorConfig(
        connector_class=OnDemandScyllaConnector,
        connector_args={'contact_points': SCYLLA_CONTACT_POINTS}
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
    'start_time': datetime.datetime.now(),
    'run_time_s': RUN_TIME_S,
    'step_time_s': STEP_TIME_S,
    'step_user_count': STEP_USER_COUNT,
    'max_rps': MAX_RPS,
    'max_users': MAX_USERS,
    'memory_backend': MEMORY_BACKEND.value,
    'num_steps': NUM_STEPS,
    'rps_per_user': RPS_PER_USER
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
