import logging
from typing import Tuple, Dict, List

import ray
from ray.actor import ActorHandle

from volga.on_demand.actors.proxy import OnDemandProxy
from volga.on_demand.actors.worker import OnDemandWorker
from volga.on_demand.on_demand_config import OnDemandConfig


logger = logging.getLogger('ray')


@ray.remote
class OnDemandCoordinator:

    def __init__(self, config: OnDemandConfig):
        self.config = config

        # TODO implement health check and restart logic for those
        self.workers_per_node = {}
        self.proxy_per_node = {}

    def start_actors(self) -> Tuple[Dict[str, ActorHandle], Dict[str, List[ActorHandle]]]:
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

            # start workers
            for worker_id in range(self.config.num_workers_per_node):
                worker = OnDemandWorker.remote(node_id, worker_id, self.config)
                if node_id in self.workers_per_node:
                    self.workers_per_node[node_id].append(worker)
                else:
                    self.workers_per_node[node_id] = [worker]

            logger.info(f'[OnDemand] Created {self.config.num_workers_per_node} workers for node {node_id}')

            # run proxy
            proxy = OnDemandProxy.remote(self.config, self.workers_per_node[node_id])
            self.proxy_per_node[node_id] = proxy
            proxy.run.remote() # it's blocking - no get

            logger.info(f'[OnDemand] Started proxy for node {node_id}')

        logger.info(f'[OnDemand] All actors started')
        return self.proxy_per_node, self.workers_per_node

    def delete_actors(self):
        # TODO
        pass