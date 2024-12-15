import time

import ray

from volga.on_demand.on_demand_config import OnDemandConfig


@ray.remote
class OnDemandWorker:

    def __init__(self, host_node_id: int, worker_id: int, config: OnDemandConfig):
        self.config = config
        self.host_node_id = host_node_id
        self.worker_id = worker_id

    async def do_work(self):
        # time.sleep(1)
        return self.host_node_id, self.worker_id
