from typing import Optional

from pydantic import BaseModel


RESOURCE_KEY_CPU = 'CPU'
RESOURCE_KEY_MEM = 'MEM'
RESOURCE_KEY_GPU = 'GPU'


class Resources(BaseModel):
    num_cpus: Optional[float]
    num_gpus: Optional[float]
    memory: Optional[str]

    @classmethod
    def from_dict(cls, resources_dict) -> 'Resources':
        return Resources(
            num_cpus=None if RESOURCE_KEY_CPU not in resources_dict else float(resources_dict[RESOURCE_KEY_CPU]),
            num_gpus=None if RESOURCE_KEY_GPU not in resources_dict else float(resources_dict[RESOURCE_KEY_GPU]),
            memory=None if RESOURCE_KEY_MEM not in resources_dict else float(resources_dict[RESOURCE_KEY_MEM]),
        )


class ResourceManager:

    # should poll Ray cluster and keep resources state in sync
    pass