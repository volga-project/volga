import time

import ray

from volga_tests.on_demand_perf.container_insights_watcher import ContainerInsightsWatcher
from volga_tests.on_demand_perf.locust_watcher import LocustWatcher
from volga.common.ray.ray_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.on_demand.actors.coordinator import create_on_demand_coordinator
from volga.on_demand.on_demand_config import OnDemandConfig

STORE_DIR = 'volga_on_demand_perf_benchmarks'
CONTAINER_INSIGHTS_STORE_PATH = 'container_insights.json'
LOCUST_STORE_PATH = 'locust.json'

container_insights_watcher = ContainerInsightsWatcher()
locust_watcher = LocustWatcher()

run_id = int(time.time())
container_insights_store_path = f'{STORE_DIR}/run-{run_id}/{CONTAINER_INSIGHTS_STORE_PATH}'
locust_store_path = f'{STORE_DIR}/run-{run_id}/{LOCUST_STORE_PATH}'

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
# ray.get(setup_sample_feature_data_ray.remote(config, num_keys)) # TODO uncomment
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
coordinator = create_on_demand_coordinator(on_demand_config)
ray.get(coordinator.start.remote())

locust_watcher.start_load_test(
    store_path=locust_store_path,
    host=HOST,
    step_user_count=STEP_USER_COUNT,
    step_time_s=STEP_TIME_S,
    run_time_s=RUN_TIME_S
)
print(f'[run-{run_id}] Started Locust')
container_insights_watcher.start(container_insights_store_path)

time.sleep(RUN_TIME_S)
locust_watcher.stop()
container_insights_watcher.stop()
print(f'[run-{run_id}] Finished On-Demand benchmark')
