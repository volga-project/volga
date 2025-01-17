import logging
import time
from typing import Tuple, Dict, List

import ray
from ray.actor import ActorHandle

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

        # TODO implement health check and restart logic
        self.servers_per_node = {}

        self.stats_manager = StatsManager(histograms=[ON_DEMAND_SERVER_LATENCY_CONFIG, ON_DEMAND_DB_LATENCY_CONFIG], counters=[ON_DEMAND_QPS_STATS_CONFIG])

    def start(self) -> Dict[str, List[ActorHandle]]:
        # TODO merge this with ray utils in streaming
        all_nodes = ray.nodes()
        for n in all_nodes:
            # TODO check we have enough resources to start workers
            _resources = n['Resources']
            # address = n['NodeManagerAddress']
            # node_name = n['NodeName']
            node_id = n['NodeID']
            alive = n['Alive']
            if not alive:
                logger.info(f'Fetched non-alive node: {n}')
                continue

            is_head = 'node:__internal_head__' in _resources
            if len(all_nodes) > 1 and is_head:
                # skip head when running in multi-node env
                continue

            # start servers
            init_futs = []
            for server_id in range(self.config.num_servers_per_node):
                server = OnDemandServer.remote(node_id, server_id, self.config)
                if node_id in self.servers_per_node:
                    self.servers_per_node[node_id].append(server)
                else:
                    self.servers_per_node[node_id] = [server]
                init_futs.append(server.init.remote())
                self.stats_manager.register_target(server)
            ray.get(init_futs)

            logger.info(f'[OnDemand] Created {self.config.num_servers_per_node} servers for node {node_id}')

            # start serving
            run_futs = []
            for node_id in self.servers_per_node:
                for server in self.servers_per_node[node_id]:
                    run_futs.append(server.run.remote())

            # ray.get(run_futs)
            time.sleep(2)
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