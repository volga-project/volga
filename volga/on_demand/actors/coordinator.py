import logging
import time
from typing import Tuple, Dict, List

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from volga.common.ray.resource_manager import ResourceManager
from volga.on_demand.actors.server import OnDemandServer
from volga.on_demand.on_demand import OnDemandSpec
from volga.on_demand.on_demand_config import OnDemandConfig
from volga.on_demand.stats import ON_DEMAND_SERVER_LATENCY_CONFIG, ON_DEMAND_DB_LATENCY_CONFIG, ON_DEMAND_QPS_STATS_CONFIG
from volga.stats.stats_manager import StatsManager

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
        server_id = 0
        for n in all_nodes:
            if is_multi_node and n.is_head:
                # skip head when running in multi-node env
                continue

            node_id = n.node_id

            # start servers
            init_futs = []
            for i in range(self.config.num_servers_per_node):

                # TODO pin to node
                options = {
                    'scheduling_strategy': NodeAffinitySchedulingStrategy(
                        node_id=node_id,
                        soft=False
                    )
                }
                server = OnDemandServer.options(**options).remote(node_id, server_id, self.config)
                if node_id in self.servers_per_node:
                    self.servers_per_node[node_id].append(server)
                else:
                    self.servers_per_node[node_id] = [server]
                init_futs.append(server.init.remote())
                self.stats_manager.register_target(server)
                server_id += 1
            ray.get(init_futs)

            logger.info(f'[OnDemand] Created {self.config.num_servers_per_node} servers for node {node_id}')

            # start serving
            for node_id in self.servers_per_node:
                for server in self.servers_per_node[node_id]:
                    server.run.remote() # no wait, run is blocking

            time.sleep(1) # TODO check the endpoints on server actors are responsive and remove this sleep
            logger.info(f'[OnDemand] Started {len(self.servers_per_node[node_id])} servers for node {node_id}')

        self.stats_manager.start()
        logger.info(f'[OnDemand] All actors started')
        return self.servers_per_node

    def register_on_demand_specs(self, specs: List[OnDemandSpec]):
        assert len(self.servers_per_node) > 0
        futs = []
        for node_id in self.servers_per_node:
            for server in self.servers_per_node[node_id]:
                futs.append(server.register_on_demand_specs.remote(specs))
        ray.get(futs)

    def stop(self):
        # TODO delete actors
        self.stats_manager.stop()


def create_on_demand_coordinator(config: OnDemandConfig) -> ActorHandle:
    # TODO use ResourceManager instance instead of static methods
    head_node = ResourceManager.fetch_head_node()
    options = {
        'scheduling_strategy': NodeAffinitySchedulingStrategy(
            node_id=head_node.node_id,
            soft=False
        )
    }
    coordinator = OnDemandCoordinator.options(**options).remote(config)
    return coordinator
