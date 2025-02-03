import time

from tests.on_demand_perf.container_insights_watcher import ContainerInsightsWatcher
from tests.on_demand_perf.locust_watcher import LocustWatcher

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
HOST = ''

print(f'[run-{run_id}] Started On-Demand benchmark')
locust_watcher.start_load_test(
    store_path=locust_store_path,
    host=HOST,
    step_user_count=STEP_USER_COUNT,
    step_time_s=STEP_TIME_S,
    run_time_s=RUN_TIME_S
)
container_insights_watcher.start(container_insights_store_path)

time.sleep(RUN_TIME_S)
locust_watcher.stop()
container_insights_watcher.stop()
print(f'[run-{run_id}] Finished On-Demand benchmark')


