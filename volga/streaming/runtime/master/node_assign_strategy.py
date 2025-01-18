import enum
import itertools
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionVertex
from volga.common.ray.resource_manager import Node, Resources


class NodeNodeAssignStrategyName(enum.Enum):
    PARALLELISM_FIRST = 'parallelism_first'
    OPERATOR_FIRST = 'operator_first'


class NodeAssignStrategy(ABC):

    @abstractmethod
    def assign_resources(self, nodes: List[Node], execution_graph: 'ExecutionGraph') -> Dict[str, Node]:
        raise NotImplementedError()

    def _has_enough_resources(self, required_resources: Resources, node: Node):
        node_res = node.resources
        if required_resources.num_cpus is not None:
            res = (node_res.num_cpus is not None and required_resources.num_cpus <= node_res.num_cpus)
        else:
            res = True

        if required_resources.memory is not None:
            res &= (node_res.memory is not None and required_resources.memory <= node_res.memory)

        if required_resources.num_gpus is not None:
            res &= (node_res.num_gpus is not None and required_resources.num_gpus <= node_res.num_gpus)
        return res

    @staticmethod
    def by_name(name: str) -> 'NodeAssignStrategy':
        if name == NodeNodeAssignStrategyName.OPERATOR_FIRST.value:
            return OperatorFirst()
        elif name == NodeNodeAssignStrategyName.PARALLELISM_FIRST.value:
            return ParallelismFirst()
        else:
            raise RuntimeError(f'Unknown NodeAssignStrategy name {name}, expected one of {[n.value for n in NodeNodeAssignStrategyName]}')


# TODO this does not work properly with remote connections, debug
# places operator instances on the same node
class ParallelismFirst(NodeAssignStrategy):

    def __init__(self):
        self.curr_node_index = 0

    def _find_matched_node(self, nodes: List[Node], resources: Resources) -> Optional[Node]:
        while self.curr_node_index < len(nodes):
            if self._has_enough_resources(resources, nodes[self.curr_node_index]):
                return nodes[self.curr_node_index]
            self.curr_node_index += 1

        if self.curr_node_index >= len(nodes):
            return None

    def assign_resources(self, nodes: List[Node], execution_graph: 'ExecutionGraph') -> Dict[str, Node]:
        total_capacity = Resources.combine(list(map(lambda n: n.resources, nodes)))
        exec_vertices_by_job_vertex_id: Dict[int, List['ExecutionVertex']] = execution_graph.execution_vertices_by_job_vertex
        all_vertices = list(itertools.chain(*exec_vertices_by_job_vertex_id.values()))
        required_capacity = Resources.combine(list(map(lambda v: v.resources, all_vertices)))

        res = {}
        max_parallelism = execution_graph.get_max_parallelism()
        for i in range(max_parallelism):
            for job_vertex_id in exec_vertices_by_job_vertex_id:
                exec_vertices = exec_vertices_by_job_vertex_id[job_vertex_id]
                if i >= len(exec_vertices):
                    # current job vertex assigned
                    continue

                exec_vertex = exec_vertices[i]
                assert exec_vertex.resources is not None
                node = self._find_matched_node(nodes, exec_vertex.resources)
                if node is None:
                    raise RuntimeError(f'Not enough resources, total capacity: {total_capacity}, required capacity: {required_capacity}')
                assert exec_vertex.execution_vertex_id not in res

                node.acquire_resources(exec_vertex.resources)

                res[exec_vertex.execution_vertex_id] = node

        return res


# places operator instances on different nodes, not shared
class OperatorFirst(NodeAssignStrategy):

    def __init__(self):
        self.node_to_operator = {}

    def _find_matched_node(self, nodes: List[Node], vertex: ExecutionVertex) -> Optional[Node]:
        i = 0
        resources = vertex.resources
        operator_id = vertex.job_vertex.vertex_id
        while i < len(nodes):
            node = nodes[i]
            belongs_to_this_operator = (node.node_id not in self.node_to_operator) or self.node_to_operator[node.node_id] == operator_id
            if belongs_to_this_operator and self._has_enough_resources(resources, node):
                self.node_to_operator[node.node_id] = operator_id
                return node
            i += 1

        if i >= len(nodes):
            return None

    def assign_resources(self, nodes: List[Node], execution_graph: 'ExecutionGraph') -> Dict[str, Node]:
        total_capacity = Resources.combine(list(map(lambda n: n.resources, nodes)))
        exec_vertices_by_job_vertex_id: Dict[
            int, List['ExecutionVertex']] = execution_graph.execution_vertices_by_job_vertex
        all_vertices = list(itertools.chain(*exec_vertices_by_job_vertex_id.values()))
        required_capacity = Resources.combine(list(map(lambda v: v.resources, all_vertices)))

        res = {}

        for job_vertex_id in exec_vertices_by_job_vertex_id:
            exec_vertices = exec_vertices_by_job_vertex_id[job_vertex_id]
            for exec_vertex in exec_vertices:
                node = self._find_matched_node(nodes, exec_vertex)
                if node is None:
                    raise RuntimeError(
                        f'Not enough resources, total capacity: {total_capacity}, required capacity: {required_capacity}')
                assert exec_vertex.execution_vertex_id not in res

                node.acquire_resources(exec_vertex.resources)

                res[exec_vertex.execution_vertex_id] = node

        return res

