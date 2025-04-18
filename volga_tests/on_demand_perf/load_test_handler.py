import json
import os
from pprint import pprint
import time
import ray
from threading import Thread
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from ray.actor import ActorHandle

from volga_tests.on_demand_perf.container_insights_api import ContainerInsightsApi
from volga_tests.on_demand_perf.locust_api import LocustApi

SLEEP_TIME_S = 1

class LoadTestHandler:

    def __init__(self, stats_store_path: str, on_demand_coordinator: ActorHandle, run_metadata: Dict):
        self._stats_watcher_thread = Thread(target=self._run_watcher_loop)
        self.running = True
        on_demand_node_names = [n['NodeManagerHostname'] for n in ray.nodes() if 'on_demand_node' in n['Resources']]
        self.container_insights_api = ContainerInsightsApi(on_demand_node_names)
        self.locust_api = LocustApi()
        self.stats_store_path = stats_store_path
        self.on_demand_coordinator = on_demand_coordinator
        self.run_metadata = run_metadata

    def start_load_test(self, host: str, step_user_count: int, step_time_s: int, run_time_s: int):
        self.locust_api.start_load_test(host, step_user_count, step_time_s, run_time_s)
        self.running = True
        self._stats_watcher_thread.start()

    def stop(self):
        self.locust_api.stop_load_test()
        self.running = False
        self._stats_watcher_thread.join(5)

    def _run_watcher_loop(self):
        while self.running:
            ts = time.time()
            
            # Use ThreadPoolExecutor to run API calls concurrently
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Submit all tasks to the executor
                container_insights_future = executor.submit(self.container_insights_api.fetch_cpu_metrics)
                locust_metrics_future = executor.submit(self.locust_api.get_stats)
                volga_metrics_future = executor.submit(lambda: ray.get(self.on_demand_coordinator.get_latest_stats.remote()))
                
                # Gather results as they complete
                container_insights_cpu_metrics_all = container_insights_future.result()
                locust_metrics = locust_metrics_future.result()
                volga_on_demand_metrics = volga_metrics_future.result()
            
            container_insights_cpu_metrics = {
                'avg': container_insights_cpu_metrics_all['avg'],
                'stdev': container_insights_cpu_metrics_all['stdev'],
                'num_pods': len(container_insights_cpu_metrics_all) - 2
            }

            pprint(f'[ContainerInsights][{ts}] {container_insights_cpu_metrics}')
            pprint(f'[Locust Stats][{ts}] {locust_metrics}')
            pprint(f'[Volga OnDemand Stats][{ts}] {volga_on_demand_metrics}')
            print('-' * 100)

            stats_update = {
                'container_insights': container_insights_cpu_metrics,
                'locust': locust_metrics,
                'volga_on_demand': volga_on_demand_metrics
            }

            self._append_stats_update(self.stats_store_path, stats_update, ts, self.run_metadata)

            time.sleep(SLEEP_TIME_S)

    @staticmethod
    def _append_stats_update(path: str, stats_update: Dict, ts: float, run_metadata: Dict):
        if os.path.isfile(path):
            with open(path, 'r') as file:
                data = json.load(file)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            data = {
                'run_metadata': run_metadata,
                'historical_stats': []
            }

        to_store = {
            'stats': stats_update,
            'timestamp': ts
        }

        data['historical_stats'].append(to_store)

        with open(path, 'w') as file:
            json.dump(data, file, indent=4)

