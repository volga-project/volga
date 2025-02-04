import statistics
from typing import Dict

import requests

BASE_URL = 'http://locust.locust:8089'


class LocustApi:

    def __init__(self):
        self.session = requests.Session()

    def start_load_test(self, host: str, step_user_count: int, step_time_s: int, run_time_s: int):
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

    def stop_load_test(self):
        self.session.get(f'{BASE_URL}/stop')

    def get_stats(self) -> Dict:
        stats = None

        max_retries = 2
        num_retries = 0
        succ = False

        while num_retries < max_retries:
            try:
                r = self.session.get(f'{BASE_URL}/stats/requests')
                stats = r.json()
                succ = True
                break
            except:
                num_retries += 1

        if not succ:
            print('Unable to fetch Locust stats')
        else:
            del stats['stats']

        return self._parse_stats(stats)

    @staticmethod
    def _parse_stats(stats: Dict) -> Dict:
        cur_p50 = stats['current_response_time_percentiles']['response_time_percentile_0.5']
        cur_p95 = stats['current_response_time_percentiles']['response_time_percentile_0.95']
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
            'total_rps': total_rps,
            'user_count': user_count,
            'avg_worker_cpu': avg_worker_cpu,
            'stdev_worker_cpu': stdev_worker_cpu,
            'total_fail_per_sec': total_fail_per_sec
        }