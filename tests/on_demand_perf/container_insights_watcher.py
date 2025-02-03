import json
import os
import statistics
import time
import random
from threading import Thread
from typing import Dict

import boto3
import datetime


class ContainerInsightsWatcher:

    def __init__(self):
        self._watcher_thread = Thread(target=self._run_watcher_loop)
        self.running = True
        self.client = boto3.client('cloudwatch')
        self.store_path = None

    def start(self, store_path: str):
        self.store_path = store_path
        self.running = True
        self._watcher_thread.start()

    def stop(self):
        self.running = False
        self._watcher_thread.join(5)

    def _run_watcher_loop(self):
        while self.running:
            ts = time.time()
            cpu_metrics = self._fetch_eks_container_insights_cpu_metrics()
            cpu_loads = list(cpu_metrics.values())
            avg_cpu = statistics.fmean(cpu_loads)
            stdev = 0
            if len(cpu_loads) > 1:
                stdev = statistics.stdev(cpu_loads)
            cpu_metrics['avg'] = avg_cpu
            cpu_metrics['stdev'] = stdev
            self._save_stats(cpu_metrics, ts)
            print(f'[ContainerInsights][{ts}] {cpu_metrics}')
            time.sleep(5)

    def _save_stats(self, stats: Dict, ts: float):
        if os.path.isfile(self.store_path):
            with open(self.store_path, 'r') as file:
                data = json.load(file)
        else:
            os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
            data = []

        to_store = {
            'container_insights_data': stats,
            'timestamp': ts
        }

        data.append(to_store)

        with open(self.store_path, 'w') as file:
            json.dump(data, file, indent=4)

    def _fetch_eks_container_insights_cpu_metrics(self) -> Dict[str, float]:
        # list metrics
        response = self.client.list_metrics(
            Namespace='ContainerInsights',
            MetricName='pod_cpu_utilization',
            Dimensions=[
                {
                    'Name': 'Namespace',
                    'Value': 'ray-system'
                },
                {
                    'Name': 'ClusterName',
                    'Value': 'volga-test-cluster'
                },
            ],
            RecentlyActive='PT3H',
        )

        filtered_pod_names = set()
        for m in response['Metrics']:
            # get 'PodName' dimension:
            for dim in m['Dimensions']:
                if dim['Name'] == 'PodName':
                    pod_name = dim['Value']
                    if 'ray-cluster-kuberay-on-demand-nodes-worker-' in pod_name:
                        filtered_pod_names.add(pod_name)

        filtered_pod_names = list(filtered_pod_names)

        response = self.client.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': f'request{int(time.time())}{random.randint(0, 1024)}',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'ContainerInsights',
                            'MetricName': 'pod_cpu_utilization',
                            'Dimensions': [
                                {
                                    'Name': 'Namespace',
                                    'Value': 'ray-system'
                                },
                                {
                                    'Name': 'ClusterName',
                                    'Value': 'volga-test-cluster'
                                },
                                {
                                    'Name': 'PodName',
                                    'Value': filtered_pod_names[i]
                                }
                            ]
                        },
                        'Period': 60,
                        'Stat': 'Average',
                    }
                } for i in range(len(filtered_pod_names))
            ],
            StartTime=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=2),
            EndTime=datetime.datetime.now(datetime.timezone.utc),
        )
        res = {}

        metric_results = response['MetricDataResults']
        assert len(metric_results) == len(filtered_pod_names)
        for i in range(len(filtered_pod_names)):
            pod_name = filtered_pod_names[i]
            metric_result = metric_results[i]
            assert metric_result['Label'] == pod_name
            last_value = metric_result['Values'][-1]
            res[pod_name] = last_value

        return res
