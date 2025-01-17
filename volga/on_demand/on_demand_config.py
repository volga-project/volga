from typing import Dict

from pydantic import BaseModel


class OnDemandConfig(BaseModel):
    client_url: str
    num_servers_per_node: int
    server_port: int
    data_service_config: Dict


DEFAULT_ON_DEMAND_CONFIG = OnDemandConfig(
    client_url='127.0.0.1',
    num_servers_per_node=1,
    server_port=1234,
    data_service_config={
        'scylla': {
            'contact_endpoints': ['127.0.0.1']
        }
    }
)
