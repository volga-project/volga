from typing import Dict, Optional

from pydantic import BaseModel

from volga.streaming.common.config.resource_config import ResourceConfig
from volga.streaming.runtime.config.scheduler_config import SchedulerConfig
from volga.streaming.runtime.network.network_config import NetworkConfig


class StreamingWorkerConfig(BaseModel):
    network_config: Optional[NetworkConfig]


class StreamingMasterConfig(BaseModel):
    resource_config: ResourceConfig
    scheduler_config: SchedulerConfig
    node_assign_strategy: str


class StreamingConfig(BaseModel):
    master_config: StreamingMasterConfig
    worker_config: StreamingWorkerConfig

    @classmethod
    def from_dict(cls, config: Dict) -> 'StreamingConfig':
        return StreamingConfig(**config)