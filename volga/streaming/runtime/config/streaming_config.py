from typing import Dict

from pydantic import BaseModel

from volga.streaming.common.config.resource_config import ResourceConfig
from volga.streaming.runtime.config.scheduler_config import SchedulerConfig


class StreamingWorkerConfig(BaseModel):
    pass


class StreamingMasterConfig(BaseModel):
    resource_config: ResourceConfig
    scheduler_config: SchedulerConfig


class StreamingConfig(BaseModel):
    master_config: StreamingMasterConfig
    worker_config_template: StreamingWorkerConfig

    @classmethod
    def from_dict(cls, config: Dict) -> 'StreamingConfig':
        return StreamingConfig(**config)