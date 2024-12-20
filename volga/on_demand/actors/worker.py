from typing import Dict, Any

import ray

from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand_config import OnDemandConfig


@ray.remote
class OnDemandWorker:

    def __init__(self, host_node_id: int, worker_id: int, config: OnDemandConfig):
        self.config = config
        self.host_node_id = host_node_id
        self.worker_id = worker_id
        DataService.init()

    async def do_work(self, feature_name: str, keys: Dict[str, Any]) -> Dict:
        feature_values = DataService.fetch_latest(feature_name=feature_name, keys=keys)
        return feature_values
