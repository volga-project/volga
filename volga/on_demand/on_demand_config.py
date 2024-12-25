from pydantic import BaseModel


class OnDemandConfig(BaseModel):
    num_workers_per_node: int
    max_ongoing_requests_per_worker: int
    proxy_port: int

DEFAULT_ON_DEMAND_CONFIG = OnDemandConfig(num_workers_per_node=2, max_ongoing_requests_per_worker=10, proxy_port=4321)
