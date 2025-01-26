import logging
from typing import Optional, List, Dict

from pydantic import BaseModel

import ray


logger = logging.getLogger('ray')

RESOURCE_KEY_CPU = 'CPU'
RESOURCE_KEY_MEM = 'MEM'
RESOURCE_KEY_GPU = 'GPU'


class Resources(BaseModel):
    num_cpus: Optional[float]
    num_gpus: Optional[float]
    memory: Optional[float]

    @classmethod
    def from_dict(cls, resources_dict) -> 'Resources':
        return Resources(
            num_cpus=None if RESOURCE_KEY_CPU not in resources_dict else float(resources_dict[RESOURCE_KEY_CPU]),
            num_gpus=None if RESOURCE_KEY_GPU not in resources_dict else float(resources_dict[RESOURCE_KEY_GPU]),
            memory=None if RESOURCE_KEY_MEM not in resources_dict else float(resources_dict[RESOURCE_KEY_MEM]),
        )

    @classmethod
    def combine(cls, resources: List['Resources']) -> 'Resources':
        res = Resources(num_cpus=None, num_gpus=None, memory=None)
        for r in resources:
            res.release_resources(r)
        return res

    def acquire_resources(self, allocated: 'Resources'):

        if allocated.num_cpus is not None:
            assert self.num_cpus is not None
            self.num_cpus -= allocated.num_cpus

        if allocated.memory is not None:
            assert self.memory is not None
            self.memory -= allocated.memory

        if allocated.num_gpus is not None:
            assert self.num_gpus is not None
            self.num_gpus -= allocated.num_gpus

    def release_resources(self, released: 'Resources'):
        if released.num_cpus is not None:
            if self.num_cpus is not None:
                self.num_cpus += released.num_cpus
            else:
                self.num_cpus = released.num_cpus

        if released.memory is not None:
            if self.memory is not None:
                self.memory += released.memory
            else:
                self.memory = released.memory

        if released.num_gpus is not None:
            if self.num_gpus is not None:
                self.num_gpus += released.num_gpus
            else:
                self.num_gpus = released.num_gpus


class Node(BaseModel):
    node_id: str
    node_name: str
    address: str
    is_head: bool
    resources: Resources
    is_streaming: bool
    is_on_demand: bool

    def acquire_resources(self, resources: Resources):
        self.resources.acquire_resources(resources)


class ResourceManager:

    def __init__(self):
        self.nodes = None

    def init(self):
        # TODO periodically check and update new/deleted nodes
        self.nodes = ResourceManager.fetch_nodes()

    # includes head node
    def get_streaming_nodes(self) -> List[Node]:
        assert self.nodes is not None
        return ResourceManager.filter_streaming_nodes(self.nodes)

    # includes head node
    def get_on_demand_nodes(self) -> List[Node]:
        assert self.nodes is not None
        return ResourceManager.filter_on_demand_nodes(self.nodes)

    def get_head_node(self) -> Node:
        assert self.nodes is not None
        return ResourceManager.filter_head_node(self.nodes)

    @staticmethod
    def filter_head_node(nodes: List[Node]) -> Node:
        res = list(filter(lambda node: node.is_head, nodes))
        assert len(res) == 1
        return res[0]

    @staticmethod
    def filter_streaming_nodes(nodes: List[Node]) -> List[Node]:
        return list(filter(lambda node: node.is_streaming, nodes))

    @staticmethod
    def filter_on_demand_nodes(nodes: List[Node]) -> List[Node]:
        return list(filter(lambda node: node.is_on_demand, nodes))

    @staticmethod
    def fetch_nodes() -> List[Node]:
        res = []
        all_nodes = ray.nodes()
        for n in all_nodes:
            _resources = n['Resources']
            is_head = 'node:__internal_head__' in _resources
            cpu = _resources.get('CPU', None)
            mem = _resources.get('memory', None)
            if mem is not None:
                mem = float(mem)
            gpu = _resources.get('GPU', None)
            if is_head:
                is_streaming = True
                is_on_demand = True
            else:
                is_streaming = True if 'streaming_node' in _resources else None
                is_on_demand = True if 'on_demand_node' in _resources else None
                if is_streaming is None and is_on_demand is None:
                    # if no custom resource specified we assume all node are for streaming
                    is_streaming = True
                    is_on_demand = False
                elif is_streaming is None:
                    is_streaming = False
                elif is_on_demand is None:
                    is_on_demand = False
                else:
                    raise RuntimeError('Node is either for on-demand or streaming workloads')

            resources = Resources(num_cpus=cpu, num_gpus=gpu, memory=mem)
            address = n['NodeManagerAddress']
            node_id = n['NodeID']
            node_name = n['NodeName']
            alive = n['Alive']
            if alive:
                res.append(Node(
                    node_id=node_id,
                    node_name=node_name,
                    address=address,
                    is_head=is_head,
                    resources=resources,
                    is_streaming=is_streaming,
                    is_on_demand=is_on_demand
                ))
            else:
                logger.info(f'Fetched non-alive node: {n}')
        return res

    @staticmethod
    def fetch_head_node() -> Node:
        return ResourceManager.filter_head_node(ResourceManager.fetch_nodes())
