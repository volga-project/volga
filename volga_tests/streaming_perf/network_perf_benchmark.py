import enum
import json
import os
import signal
import subprocess
import time
from typing import Dict, Optional, List

import pandas as pd
import ray

import seaborn as sns
import matplotlib.pyplot as plt

from volga_tests.test_streaming_wordcount import TestWordCount
from volga.streaming.runtime.network.test_local_transfer import TestLocalTransfer
from volga.streaming.runtime.network.test_remote_transfer import TestRemoteTransfer
from volga.common.ray.ray_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV

RETRIES_PER_RUN = 2


# which test scenario to run
class RunScenario(enum.Enum):
    NETWORK_LOCAL = 'network_local' # local network (without transfer actors), tested locally, n-to-n
    NETWORK_REMOTE = 'network_remote' # remote network (with transfer actors), tested locally, n-to-n
    NETWORK_CLUSTER = 'network_cluster' # remote network (with transfer actors), tested on a cluster
    WORDCOUNT_LOCAL = 'wordcount_local' # wordcount, tested locally
    WORDCOUNT_CLUSTER = 'wordcount_cluster' # wordcount, tested on a cluster


RUN_SCENARIO = RunScenario.WORDCOUNT_CLUSTER

# in network_cluster setting, we run on c5.2xlarge instances which have 8 vCPUs. We reserve 2vCPUs per worker
NUM_WORKERS_PER_NODE = 4

RUN_DURATION_S = 25 # how long a single test run lasts, this should be more than aggregation warmup thresh (10s by def)

STATS_STORE_DIR = 'volga_network_perf_benchmarks'
# 16 * 8 = 80 / 2 = 40 / 2 = 20
PARAMS_MATRIX = {
    'parallelism': [1] + [*range(4, 33, 4)],
    # 'parallelism': [*range(1, 11)],
    # 'parallelism': [*range(100, 200, 8)],
    'msg_size': [32, 256, 1024],
    # 'msg_size': [32],
    # 'msg_size': [32, 256, 1024],
    # 'msg_size': [32],
    'batch_size': [1, 10, 100, 1000]
    # 'batch_size': [100]
}


def store_run_stats(
    res_file_name: str,
    parallelism: int,
    num_msgs: int,
    msg_size: int,
    batch_size: int,
    avg_throughput: float,
    latency_stats: Dict,
    hist_throughput: List,
    hist_latency: List,
):
    to_store = {
        'msg_size': msg_size,
        'batch_size': batch_size,
        'parallelism': parallelism,
        'throughput': avg_throughput,
        'latency_ms': latency_stats,
        'num_msgs': num_msgs,
        'hist_throughput': hist_throughput,
        'hist_latency': hist_latency
    }

    meta = {
        'scenario': RUN_SCENARIO.value
    }

    if os.path.isfile(res_file_name):
        with open(res_file_name, 'r') as file:
            data = json.load(file)
    else:
        os.makedirs(os.path.dirname(res_file_name), exist_ok=True)
        data = {
            'meta': meta,
            'stats': []
        }

    data['stats'].append(to_store)

    with open(res_file_name, 'w') as file:
        json.dump(data, file, indent=4)


def benchmark(rerun_file: Optional[str] = None):
    res = {}
    existing_runs = set()
    if rerun_file is None:
        res_file_name = f'{STATS_STORE_DIR}/benchmark_{RUN_SCENARIO.value}_{int(time.time())}.json'
    else:
        res_file_name = rerun_file
        with open(rerun_file, 'r') as file:
            data = json.load(file)['stats']
            for e in data:
                existing_runs.add((e['msg_size'], e['batch_size'], e['parallelism']))

    num_runs = len(PARAMS_MATRIX['msg_size']) * len(PARAMS_MATRIX['batch_size']) * len(PARAMS_MATRIX['parallelism'])
    print(f'Skipping {len(existing_runs)} existing runs')
    num_runs -= len(existing_runs)
    print(f'Executing {num_runs} runs')
    run_id = 1
    port_fwd_pid = None

    for parallelism in PARAMS_MATRIX['parallelism']:
        for msg_size in PARAMS_MATRIX['msg_size']:
            for batch_size in PARAMS_MATRIX['batch_size']:
                if (msg_size, batch_size, parallelism) in existing_runs:
                    continue
                if parallelism == 0:
                    raise RuntimeError('parallelism can not be 0')
                run_res = (-1, -1, -1)
                print(f'Executing run {run_id}/{num_runs}: msg_size={msg_size}, batch_size={batch_size}, parallelism={parallelism}')
                num_retires = 0
                run_succ = False
                while not run_succ and num_retires < RETRIES_PER_RUN:
                    try:
                        if RUN_SCENARIO == RunScenario.NETWORK_REMOTE or RUN_SCENARIO == RunScenario.NETWORK_CLUSTER:
                            if RUN_SCENARIO == RunScenario.NETWORK_CLUSTER:
                                if port_fwd_pid is None:
                                    port_fwd_pid = kubectl_port_forward()
                                multinode = True
                                ray_addr = RAY_ADDR
                                runtime_env = REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
                            else:
                                multinode = False
                                ray_addr = None
                                runtime_env = None
                                # test connectivity and restart cluster if needed
                                # TODO test connectivity before restarting
                                subprocess.run('ray stop && ray start --head', shell=True, check=True, capture_output=False,
                                               encoding='utf-8')

                            t = TestRemoteTransfer()
                            avg_throughput, latency_stats, num_msgs, hist_throughput, hist_latency = t.test_nw_to_nr_star_on_ray(
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
                        elif RUN_SCENARIO == RunScenario.NETWORK_LOCAL:
                            t = TestLocalTransfer()
                            avg_throughput, latency_stats, num_msgs, hist_throughput, hist_latency = t.test_n_all_to_all_on_local_ray(
                                n=parallelism,
                                msg_size=msg_size,
                                batch_size=batch_size,
                                run_for_s=RUN_DURATION_S
                            )

                        elif RUN_SCENARIO == RunScenario.WORDCOUNT_LOCAL or RUN_SCENARIO == RunScenario.WORDCOUNT_CLUSTER:
                            t = TestWordCount()
                            if RUN_SCENARIO == RunScenario.WORDCOUNT_CLUSTER:
                                if port_fwd_pid is None:
                                    port_fwd_pid = kubectl_port_forward()

                                ray_addr = RAY_ADDR
                                runtime_env = REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
                            else:
                                ray_addr = None
                                runtime_env = None

                            avg_throughput, latency_stats, num_msgs, hist_throughput, hist_latency = t.test_wordcount(
                                parallelism=parallelism,
                                word_length=msg_size,
                                batch_size=batch_size,
                                run_for_s=RUN_DURATION_S,
                                ray_addr=ray_addr,
                                runtime_env=runtime_env
                            )
                        else:
                            raise RuntimeError('Unsupported run scenario')
                        run_res = (avg_throughput, latency_stats, num_msgs)
                        store_run_stats(
                            res_file_name=res_file_name,
                            parallelism=parallelism,
                            num_msgs=num_msgs,
                            msg_size=msg_size,
                            batch_size=batch_size,
                            avg_throughput=avg_throughput,
                            latency_stats=latency_stats,
                            hist_throughput=hist_throughput,
                            hist_latency=hist_latency
                        )
                        run_succ = True
                        time.sleep(2)
                    except Exception as e:
                        num_retires += 1
                        print(f'Failed p={parallelism}: {e}')
                        ray.shutdown()
                        # restart kubefwd for cluster setting
                        if RUN_SCENARIO == RunScenario.NETWORK_CLUSTER or RUN_SCENARIO == RunScenario.WORDCOUNT_CLUSTER:
                            if port_fwd_pid is not None:
                                print(f'Killed old port fwd at pid {port_fwd_pid}')
                                os.kill(port_fwd_pid, signal.SIGTERM)
                            time.sleep(1)
                            print('Restarting port fwd')
                            port_fwd_pid = kubectl_port_forward()

                key = f'msg_size={msg_size},batch_size={batch_size},p={parallelism}'
                res[key] = run_res
                print(f'Executing run {run_id}/{num_runs}... Done')
                run_id += 1

    for key in res:
        avg_throughput, latency_stats, run_duration = res[key]
        if avg_throughput < 0:
            print(f'{key}: Failed')
        else:
            print(f'{key}: {avg_throughput} msg/s, {latency_stats}, {run_duration} s')


def plot(filename: str):
    with open(filename, 'r') as file:
        data = json.load(file)['stats']
        processed = []

        for e in data:
            # if e['msg_size'] == 1024 and e['batch_size'] == 10:
            #     continue
            # if e['parallelism'] > 9:
            #     continue
            skip = False
            for k in e['latency_ms']:
                val = e['latency_ms'][k]
                # if val > 500:
                #     # val = 300
                #     skip = True
                if val > 300:
                    val = 300
                    # skip = True
                e[f'latency_{k}_ms'] = val

            if skip:
                continue
            del e['latency_ms']
            del e['num_msgs']
            processed.append(e)
        df = pd.DataFrame(processed)
        df['throughput_e6'] = df['throughput'] / 1000000

        titles = [
            'payload_size=32 | msg_size=123',
            'payload_size=256 | msg_size=573',
            'payload_size=1024 | msg_size=2109',
        ]
        title_font_size = 9
        g1 = sns.FacetGrid(df, row='msg_size', hue='batch_size')
        g1.map(sns.lineplot, 'parallelism', 'throughput_e6')
        # g1.add_legend()
        for idx, ax in enumerate(g1.axes.flat):
            ax.set_title(titles[idx], fontsize=title_font_size)

        # df['latency_p99_ms'].mask((df['msg_size'] == 1024) & (df['batch_size'] == 10), 0, inplace=True)
        g2 = sns.FacetGrid(df, row='msg_size', hue='batch_size')
        g2.map(sns.lineplot, 'parallelism', 'latency_p99_ms')
        # g2.add_legend()
        for idx, ax in enumerate(g2.axes.flat):
            ax.set_title(titles[idx], fontsize=title_font_size)

        # df['latency_avg_ms'].mask((df['msg_size'] == 1024) & (df['batch_size'] == 10), 0, inplace=True)
        g3 = sns.FacetGrid(df, row='msg_size', hue='batch_size')
        g3.map(sns.lineplot, 'parallelism', 'latency_avg_ms')
        g3.add_legend()
        for idx, ax in enumerate(g3.axes.flat):
            ax.set_title(titles[idx], fontsize=title_font_size)

        # plt.savefig('local_lat_avg.png',dpi=400)
        plt.show()


# To list running port-forwards
# kubectl get svc -o json | jq '.items[] | {name:.metadata.name, p:.spec.ports[] } | select( .p.nodePort != null ) | "\(.name): localhost:\(.p.nodePort) -> \(.p.port) -> \(.p.targetPort)"'

# list pids on port 12345
# sudo lsof -n -i :12345 | grep LISTEN
def kubectl_port_forward() -> int:
    p = subprocess.Popen('kubectl port-forward -n ray-system svc/ray-cluster-kuberay-head-svc 12345:10001', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    os.set_blocking(p.stderr.fileno(), False)
    time.sleep(5)
    err = p.stderr.readline()
    if len(err) != 0:
        raise RuntimeError(f'Unable to port-forward: {err}')
    print(f'Started kubectl port-forward pid: {p.pid}')
    return p.pid


# removes specified data points from saved file so we can re-run them
def remove_data_points(res_file_name, to_rem):
    with open(res_file_name, 'r') as file:
        data = json.load(file)

    stats = data['stats']
    proced = []
    for e in stats:
        tup = (e['msg_size'], e['batch_size'], e['parallelism'])
        if tup not in to_rem:
            proced.append(e)

    data['stats'] = proced
    with open(res_file_name, 'w') as file:
        json.dump(data, file, indent=4)


# benchmark(f'{STATS_STORE_DIR}/benchmark_network_remote_1732895521.json')
# plot(f'{STATS_STORE_DIR}/benchmark_wordcount_cluster_1732707615.json')
# plot(f'{STATS_STORE_DIR}/benchmark_wordcount_cluster_1732780219.json')
# plot(f'{STATS_STORE_DIR}/benchmark_wordcount_cluster_1733049860.json')
# plot(f'{STATS_STORE_DIR}/benchmark_network_local_1732963400.json')
# plot(f'{STATS_STORE_DIR}/benchmark_network_cluster_1733121692.json')
# benchmark(f'{STATS_STORE_DIR}/benchmark_network_local_1732963400.json')
# plot(f'{STATS_STORE_DIR}/benchmark_wordcount_local_1733144393.json')
# benchmark(f'{STATS_STORE_DIR}/benchmark_wordcount_cluster_many_short_2.json')

plot(f'{STATS_STORE_DIR}/benchmark_wordcount_cluster_final.json')
# plot(f'{STATS_STORE_DIR}/benchmark_wordcount_local_final.json')