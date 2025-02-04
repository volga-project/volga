import statistics
import time
import random
from typing import Dict

import boto3
import datetime


class ContainerInsightsApi:

    def __init__(self):
        self.boto_client = boto3.client('cloudwatch')

    def fetch_cpu_metrics(self) -> Dict[str, float]:
        # list metrics
        response = self.boto_client.list_metrics(
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

        response = self.boto_client.get_metric_data(
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

        cpu_loads = list(res.values())
        avg_cpu = statistics.fmean(cpu_loads)
        stdev = 0
        if len(cpu_loads) > 1:
            stdev = statistics.stdev(cpu_loads)
        res['avg'] = avg_cpu
        res['stdev'] = stdev

        return res

