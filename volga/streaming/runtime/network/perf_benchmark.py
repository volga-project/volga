import json
import os
import time
from typing import Dict

import pandas as pd
import ray

import seaborn as sns
import matplotlib.pyplot as plt

from volga.streaming.runtime.network.test_remote_transfer import TestRemoteTransfer
from volga.streaming.runtime.network.testing_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV

NUM_WORKERS_PER_NODE = 8 # in remote setting, we run on c5.2xlarge instances which have 8 vCPUs, hence the num of workers

REMOTE_OR_LOCAL = False # true if we test on remote cluster, false on local

RUN_DURATION_S = 25 # how long a single test run lasts, this should be more than aggregation warmup thresh (10s by def)

STATS_STORE_DIR = 'volga_network_perf_benchmarks'

PARAMS_MATRIX = {
    'parallelism': [*range(1, 6)],
    'msg_size': [32, 256, 1024],
    # 'batch_size': [1000]
    'batch_size': [1, 10, 100, 1000]
}


def store_run_stats(
    res_file_name: str,
    parallelism: int,
    num_msgs: int,
    msg_size: int,
    batch_size: int,
    avg_throughput: float,
    latency_stats: Dict
):
    to_store = {
        'msg_size': msg_size,
        'batch_size': batch_size,
        'parallelism': parallelism,
        'throughput': avg_throughput,
        'latency_ms': latency_stats,
        'num_msgs': num_msgs,
    }

    if os.path.isfile(res_file_name):
        with open(res_file_name, 'r') as file:
            data = json.load(file)
    else:
        os.makedirs(os.path.dirname(res_file_name), exist_ok=True)
        data = []

    data.append(to_store)

    with open(res_file_name, 'w') as file:
        json.dump(data, file, indent=4)


# TODO figure out how to parametrize batch_size+parallelism+msg_size variations
def throughput_benchmark():
    t = TestRemoteTransfer()
    res_file_name = f'{STATS_STORE_DIR}/benchmark_{int(time.time())}.json'
    res = {}

    for msg_size in PARAMS_MATRIX['msg_size']:
        for batch_size in PARAMS_MATRIX['batch_size']:
            for parallelism in PARAMS_MATRIX['parallelism']:
                if parallelism == 0:
                    raise RuntimeError('parallelism can not be 0')
                run_res = (-1, -1, -1)
                try:
                    if REMOTE_OR_LOCAL:
                        multinode = True
                        ray_addr = RAY_ADDR
                        runtime_env = REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
                    else:
                        multinode = False
                        ray_addr = None
                        runtime_env = None

                    avg_throughput, latency_stats, num_msgs = t.test_nw_to_nr_star_on_ray(
                        nw=parallelism,
                        nr=parallelism,
                        num_workers_per_node=NUM_WORKERS_PER_NODE,
                        msg_size=msg_size,
                        batch_size=batch_size,
                        run_for_s=RUN_DURATION_S,
                        ray_addr=ray_addr,
                        runtime_env=runtime_env,
                        multinode=multinode
                    )

                    store_run_stats(
                        res_file_name=res_file_name,
                        parallelism=parallelism,
                        num_msgs=num_msgs,
                        msg_size=msg_size,
                        batch_size=batch_size,
                        avg_throughput=avg_throughput,
                        latency_stats=latency_stats
                    )
                    time.sleep(2)
                except Exception as e:
                    print(f'Failed p={parallelism}: {e}')
                    ray.shutdown()

                key = f'msg_size={msg_size},batch_size={batch_size},p={parallelism}'
                res[key] = run_res

    for key in res:
        avg_throughput, latency_stats, run_duration = res[key]
        if avg_throughput < 0:
            print(f'{key}: Failed')
        else:
            print(f'{key}: {avg_throughput} msg/s, {latency_stats}, {run_duration} s')


def plot(filename: str):
    with open(filename, 'r') as file:
        data = json.load(file)
        processed = []

        for e in data:
            for k in e['latency_ms']:
                e[f'latency_{k}_ms'] = e['latency_ms'][k]
            del e['latency_ms']
            del e['num_msgs']
            processed.append(e)

        df = pd.DataFrame(processed)

        # msg_sizes = df['msg_size'].unique().tolist()
        # batch_sizes = df['batch_size'].unique().tolist()
        # nrows = len(msg_sizes)
        # ncols = len(batch_sizes)
        # fig, axes = plt.subplots(nrows, ncols, sharex=True, figsize=(16, 8))
        # fig.suptitle('Throughput stats')
        # for col in range(len(msg_sizes)):
        #     msg_size = msg_sizes[col]
        #     for row in range(len(batch_sizes)):
        #         batch_size = batch_sizes[row]
        #         data = df[(df['msg_size'] == msg_size) & (df['batch_size'] == batch_size)]
        #         seaborn.lineplot(ax=axes[row], data=data, x='parallelism', y='throughput')
        #         throughput_data = throughput_data[['throughput', 'parallelism']]
        #         print(msg_size, batch_size)
        #         print(throughput_data)

        throughput_df = df[['throughput', 'parallelism', 'msg_size', 'batch_size']]
        g = sns.FacetGrid(throughput_df, row='msg_size', hue='batch_size')
        g.map(sns.lineplot, 'parallelism', 'throughput')
        g.add_legend()

        latency_df = df[['latency_p99_ms', 'parallelism', 'msg_size', 'batch_size']]
        g2 = sns.FacetGrid(latency_df, row='msg_size', hue='batch_size')
        g2.map(sns.lineplot, 'parallelism', 'latency_p99_ms')
        g2.add_legend()
        plt.show()

# throughput_benchmark()
plot(f'{STATS_STORE_DIR}/benchmark_1731228815.json')

# 1<->1: 77279.62991009754 msg/s, 1.2940020561218262 s
# 2<->2: 159084.37156745538 msg/s, 2.5143890380859375 s
# 3<->3: 198417.67251588262 msg/s, 4.535886287689209 s
# 4<->4: 288623.8268141708 msg/s, 5.543547868728638 s
# 5<->5: 353878.0211981364 msg/s, 7.0645811557769775 s
# 6<->6: 377512.22861806024 msg/s, 9.536114931106567 s
# 7<->7: 445156.1366645908 msg/s, 11.007373809814453 s
# 8<->8: 487622.0946902425 msg/s, 13.124917984008789 s
# 9<->9: 557218.074788173 msg/s, 14.5364990234375 s
# 10<->10: 571694.5873933224 msg/s, 17.491857051849365 s
# 11<->11: 688648.7879781058 msg/s, 17.570640087127686 s
# 12<->12: 724608.2452501928 msg/s, 19.872807264328003 s
# 13<->13: 809510.9968577144 msg/s, 20.876801013946533 s
# 14<->14: 874072.8980508689 msg/s, 22.42375898361206 s
# 15<->15: 918634.05002135 msg/s, 24.492887020111084 s

# 0<->0: 78195.00091631038 msg/s, 1.2788541316986084 s
# 5<->5: 270598.81952627556 msg/s, 9.238769054412842 s
# 10<->10: 505774.6804948192 msg/s, 19.771650075912476 s
# 15<->15: 752058.2860982413 msg/s, 29.917893886566162 s
# 20<->20: 926800.8961873061 msg/s, 43.15921592712402 s

# -- new --
# 1<->1: 77391.47987838203 msg/s, 1.2921319007873535 s
# 5<->5: 274153.7433745098 msg/s, 9.11897087097168 s
# 10<->10: 527487.0838855837 msg/s, 18.957810163497925 s
# 15<->15: 745625.5125779167 msg/s, 30.176006078720093 s
# 20<->20: 943023.6885939682 msg/s, 42.41674995422363 s
# 25<->25: 1054430.353377645 msg/s, 59.27371096611023 s
# 30<->30: 1714816.090105296 msg/s, 52.48376226425171 s
# 35<->35: 2121138.693458819 msg/s, 57.75199913978577 s
# 40<->40: 2237966.1073087333 msg/s, 71.49348664283752 s
# 45<->45: 2472339.831106908 msg/s, 81.90621590614319 s
# 50<->50: 2785009.5203105453 msg/s, 89.76629996299744 s