import json
import os
import time
from threading import Thread
from typing import Dict

import requests

BASE_URL = 'http://localhost:8089'

STEP_LENGTH_S = 120


class LocustWatcher:

    def __init__(self):
        self.stats = []
        self._watcher_thread = Thread(target=self._run_watcher_loop)
        self.running = False
        self.store_path = None

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
                r = requests.post(f'{BASE_URL}/spawn', p)
                res = r.json()
                assert res['success'] is True
                succ = True
                break
            except:
                num_retries += 1

        if not succ:
            raise RuntimeError('Unable to start Locust loadtest')

        self.running = True
        self._watcher_thread.start()

    def stop(self):
        self.running = False
        self._watcher_thread.join(5)
        requests.get(f'{BASE_URL}/stop')

    def _get_stats(self) -> Dict:
        r = requests.get(f'{BASE_URL}/stats/requests')
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
                print(f'[Locust][{ts}] {self._parse_locust_stats(stats)}')
                self._save_stats(stats, ts)
            time.sleep(5)

    def _parse_locust_stats(self, stats: Dict) -> Dict:
        return stats # TODO

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