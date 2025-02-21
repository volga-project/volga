import time

import ray

from volga.common.ray.ray_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.config import OnDemandConfig
from volga_tests.on_demand_perf.load_test_handler import LoadTestHandler
from volga.on_demand.testing_utils import setup_sample_feature_data_ray

STORE_DIR = 'volga_on_demand_perf_benchmarks'

run_id = int(time.time())

RUN_TIME_S = 125
STEP_TIME_S = 30
RPS_PER_USER = 10
MAX_RPS = 1000
MAX_USERS = int(MAX_RPS/RPS_PER_USER)
NUM_STEPS = int(RUN_TIME_S/STEP_TIME_S)
STEP_USER_COUNT = int(MAX_USERS/NUM_STEPS)
HOST = 'http://k8s-raysyste-volgaond-3637bbe071-237137006.ap-northeast-1.elb.amazonaws.com/'

print(f'[run-{run_id}] Started On-Demand benchmark')
ray.init(address=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV)
on_demand_config = OnDemandConfig(
    # client_url='127.0.0.1',
    # client_url='on-demand-service.ray-system.svc.cluster.local',
    client_url='127.1.27.4',
    num_servers_per_node=2,
    server_port=1122,
    data_service_config={
        'scylla': {
            # 'contact_points': ['127.0.0.1']
            'contact_points': ['scylla-client.scylla.svc.cluster.local']
        }
    }
)
ray.get(setup_sample_feature_data_ray.remote(on_demand_config, 10000))
coordinator = create_on_demand_coordinator(on_demand_config)
ray.get(coordinator.start.remote())

stats_store_path = f'{STORE_DIR}/run-{run_id}.json'
load_test_handler = LoadTestHandler(stats_store_path, coordinator)

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
