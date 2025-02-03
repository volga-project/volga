import json
import os
import statistics
import time
from threading import Thread
from typing import Dict

import requests

BASE_URL = 'http://locust.locust:8089'


class LocustWatcher:

    def __init__(self):
        self.stats = []
        self._watcher_thread = Thread(target=self._run_watcher_loop)
        self.running = False
        self.store_path = None
        self.session = requests.Session()

    def start_load_test(self, store_path: str, host: str, step_user_count: int, step_time_s: int, run_time_s: int):
        self.store_path = store_path
        p = {
            'user_count': step_user_count,
            'spawn_rate': step_time_s, # this is a hack - we pass step_time_s as 'spawn_rate' param, load test parses it accordingly
            'host': host,
            'run_time': run_time_s
        }

        max_retries = 2
        num_retries = 0
        succ = False

        while num_retries < max_retries:
            try:
                r = self.session.post(f'{BASE_URL}/swarm', p)
                res = r.json()
                assert res['success'] is True
                succ = True
                break
            except Exception as e:
                print(e)
                num_retries += 1

        if not succ:
            raise RuntimeError('Unable to start Locust loadtest')

        self.running = True
        self._watcher_thread.start()

    def stop(self):
        self.running = False
        self._watcher_thread.join(5)
        self.session.get(f'{BASE_URL}/stop')

    def _get_stats(self) -> Dict:
        r = self.session.get(f'{BASE_URL}/stats/requests')
        return r.json()

    def _run_watcher_loop(self):
        while self.running:
            ts = None
            stats = None

            max_retries = 2
            num_retries = 0
            succ = False

            while num_retries < max_retries:
                try:
                    ts = time.time()
                    stats = self._get_stats()
                    succ = True
                    break
                except:
                    num_retries += 1

            if not succ:
                print('Unable to fetch Locust stats')
            else:
                if 'stats' in  stats:
                    del stats['stats']
                print(f'[Locust][{ts}] {self._parse_locust_stats(stats)}')
                self._save_stats(stats, ts)

            req_latency = time.time() - ts
            print(f'[Locust Stats Req Latency] {req_latency}')
            sleep = 5 - req_latency
            if sleep > 0:
                time.sleep(5)

    def _parse_locust_stats(self, stats: Dict) -> Dict:
        cur_p50 = stats['current_response_time_percentiles']['response_time_percentile_0.5']
        cur_p95 = stats['current_response_time_percentiles']['response_time_percentile_0.95']

        # total_p95 = stats['stats'][0]['response_time_percentile_0.95']
        # total_p99 = stats['stats'][0]['response_time_percentile_0.99']

        # cur_rps = stats['stats'][0]['current_rps']
        # current_fail_per_sec = stats['stats'][0]['current_fail_per_sec']
        total_rps = stats['total_rps']
        total_fail_per_sec = stats['total_fail_per_sec']
        user_count = stats['user_count']

        workers_cpu = [w['cpu_usage'] for w in stats['workers']]
        avg_worker_cpu = statistics.fmean(workers_cpu)
        stdev_worker_cpu = 0
        if len(workers_cpu) > 1:
            stdev_worker_cpu = statistics.stdev(workers_cpu)

        return {
            'cur_p50': cur_p50,
            'cur_p95': cur_p95,
            # 'total_p95': total_p95,
            # 'total_p99': total_p99,
            # 'cur_rps': cur_rps,
            # 'current_fail_per_sec': current_fail_per_sec,
            'total_rps': total_rps,
            'user_count': user_count,
            'avg_worker_cpu': avg_worker_cpu,
            'stdev_worker_cpu': stdev_worker_cpu,
            'total_fail_per_sec': total_fail_per_sec
        }

    def _save_stats(self, stats: Dict, ts: float):
        if os.path.isfile(self.store_path):
            with open(self.store_path, 'r') as file:
                data = json.load(file)
        else:
            os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
            data = []

        to_store = {
            'locust_data': stats,
            'timestamp': ts
        }

        data.append(to_store)

        with open(self.store_path, 'w') as file:
            json.dump(data, file, indent=4)