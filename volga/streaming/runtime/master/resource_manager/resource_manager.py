import logging
from typing import Optional, List, Dict

from pydantic import BaseModel

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
import ray

from volga.streaming.runtime.master.resource_manager.node_assign_strategy import NodeAssignStrategy

logger = logging.getLogger("ray")

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

    def decrease_resources(self, allocated: 'Resources'):
        self.num_cpus -= allocated.num_cpus
        self.memory -= allocated.memory
        if allocated.num_gpus is not None:
            if self.num_gpus is None:
                raise RuntimeError('num_gpus is None')
            self.num_gpus -= allocated.num_gpus


class Node(BaseModel):
    node_id: str
    node_name: str
    address: str
    is_head: bool
    resources: Resources

    def allocate_execution_vertex(self, vertex: ExecutionVertex):
        vertex_resources = vertex.resources
        if vertex_resources is None:
            raise RuntimeError('Vertex resources is None')
        self.resources.decrease_resources(vertex_resources)


class ResourceManager:

    def __init__(self):
        self.nodes = None

    def init(self):
        # TODO periodically check and update new/deleted nodes
        self.nodes = self._fetch_nodes()

    def _fetch_nodes(self) -> List[Node]:
        res = []
        all_nodes = ray.nodes()
        for n in all_nodes:
            _resources = n['Resources']
            is_head = 'node:__internal_head__' in _resources
            cpu = _resources['CPU']
            mem = _resources['memory']
            gpu = _resources.get('GPU', None)
            resources = Resources(num_cpus=cpu, num_gpus=gpu, memory=mem)
            address = n['NodeManagerAddress']
            node_id = n['NodeID']
            node_name = n['NodeName']
            alive = n['Alive']
            if alive:
                res.append(Node(node_id=node_id, node_name=node_name, address=address, is_head=is_head, resources=resources))
            else:
                logger.info(f'Fetched non-alive node: {n}')
        return res

    def assign_resources(self, execution_graph: ExecutionGraph, strategy: NodeAssignStrategy) -> Dict[str, Node]:
        if self.nodes is None:
            raise RuntimeError('ResourceManager not inited')
        return strategy.assign_resources(self.nodes, execution_graph)