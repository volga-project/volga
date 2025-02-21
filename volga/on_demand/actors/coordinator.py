import logging
import time
from typing import Tuple, Dict, List

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.common.ray.resource_manager import ResourceManager
from volga.on_demand.actors.server import OnDemandServer
from volga.on_demand.config import OnDemandConfig
from volga.on_demand.stats import ON_DEMAND_SERVER_LATENCY_CONFIG, ON_DEMAND_DB_LATENCY_CONFIG, ON_DEMAND_QPS_STATS_CONFIG
from volga.stats.stats_manager import StatsManager  
from volga.api.feature import Feature
logger = logging.getLogger('ray')


@ray.remote
class OnDemandCoordinator:

    def __init__(self, config: OnDemandConfig):
        self.config = config

        # TODO implement health check
        self.servers_per_node = {}

        self.stats_manager = StatsManager(histograms=[ON_DEMAND_SERVER_LATENCY_CONFIG, ON_DEMAND_DB_LATENCY_CONFIG], counters=[ON_DEMAND_QPS_STATS_CONFIG])

    def start(self) -> Dict[str, List[ActorHandle]]:
        # TODO use ResourceManager instance
        all_nodes = ResourceManager.filter_on_demand_nodes(ResourceManager.fetch_nodes())

        is_multi_node = len(all_nodes) > 1
        init_futs = []
        node_index = 1
        for n in all_nodes:
            if is_multi_node and n.is_head:
                # skip head when running in multi-node env
                continue

            node_id = n.node_id

            for i in range(self.config.num_servers_per_node):
                options = {
                    'scheduling_strategy': NodeAffinitySchedulingStrategy(
                        node_id=node_id,
                        soft=False
                    )
                }
                server_id = f'{node_index}_{i + 1}' # 1-based

                server = OnDemandServer.options(**options).remote(node_id, server_id, self.config)
                if node_id in self.servers_per_node:
                    self.servers_per_node[node_id].append(server)
                else:
                    self.servers_per_node[node_id] = [server]
                init_futs.append(server.init.remote())
                self.stats_manager.register_target(str(server_id), server)

            node_index += 1

        ray.get(init_futs)


        # start serving
        for node_id in self.servers_per_node:
            for server in self.servers_per_node[node_id]:
                server.run.remote() # no wait, run is blocking

        logger.info(f'[OnDemand] Launched {len(init_futs)} servers, {self.config.num_servers_per_node} per node')

        time.sleep(1)  # TODO check the endpoints on server actors are responsive and remove this sleep

        self.stats_manager.start()
        logger.info(f'[OnDemand] All actors started')
        return self.servers_per_node

    def stop(self):
        # TODO delete actors
        self.stats_manager.stop()

    def register_features(self, features: Dict[str, Feature]):
        futs = []
        for node_id in self.servers_per_node:
            for server in self.servers_per_node[node_id]:
                futs.append(server.register_features.remote(features))
        ray.get(futs)

    def get_latest_stats(self) -> Dict:
        latest_stats =  self.stats_manager.get_latest_stats()

        # parse
        res = {}
        if ON_DEMAND_QPS_STATS_CONFIG.name in latest_stats['counters']:
            qps = latest_stats['counters'][ON_DEMAND_QPS_STATS_CONFIG.name]
            res['qps'] = qps[1]
            res['qps_stdev'] = qps[2]

        if ON_DEMAND_DB_LATENCY_CONFIG.name in latest_stats['histograms']:
            db_latency = latest_stats['histograms'][ON_DEMAND_DB_LATENCY_CONFIG.name][1]
            for k, v in db_latency.items():
                res[f'db_{k}'] = v

        if ON_DEMAND_SERVER_LATENCY_CONFIG.name in latest_stats['histograms']:
            server_latency = latest_stats['histograms'][ON_DEMAND_SERVER_LATENCY_CONFIG.name][1]
            for k, v in server_latency.items():
                res[f'server_{k}'] = v

        return res


def create_on_demand_coordinator(config: OnDemandConfig) -> ActorHandle:
    # TODO use ResourceManager instance instead of static methods
    head_node = ResourceManager.fetch_head_node()
    options = {
        'scheduling_strategy': NodeAffinitySchedulingStrategy(
            node_id=head_node.node_id,
            soft=False
        ),
        'num_cpus': 0 # to schedule on head
    }
    coordinator = OnDemandCoordinator.options(**options).remote(config)
    return coordinator
