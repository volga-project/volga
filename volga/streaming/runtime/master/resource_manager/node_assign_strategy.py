from abc import ABC, abstractmethod
from typing import List, Dict

from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph, ExecutionVertex
from volga.streaming.runtime.master.resource_manager.resource_manager import Node, Resources


class NodeAssignStrategy(ABC):

    @abstractmethod
    def assign_resources(self, nodes: List[Node], execution_graph: ExecutionGraph) -> Dict[str, Node]:
        raise NotImplementedError()

    def _has_enough_resources(self, required_resources: Resources, node: Node):
        node_res = node.resources
        res = required_resources.num_cpus <= node_res.num_cpus and required_resources.memory <= node_res.memory
        if required_resources.num_gpus is not None:
            res &= (node_res.num_gpus is not None and required_resources.num_gpus <= node_res.num_gpus)
        return res


class ParallelismFirst(NodeAssignStrategy):

    def __init__(self):
        self.curr_node_index = 0

    def _find_matched_node(self, nodes: List[Node], resources: Resources) -> Node:
        while self.curr_node_index < len(nodes):
            if self._has_enough_resources(resources, nodes[self.curr_node_index]):
                return nodes[self.curr_node_index]
            self.curr_node_index += 1

        if self.curr_node_index >= len(nodes):
            raise RuntimeError(f'Not enough resources, more required {resources}')

    def assign_resources(self, nodes: List[Node], execution_graph: ExecutionGraph) -> Dict[str, Node]:
        res = {}
        exec_vertices_by_job_vertex_id: Dict[int, List[ExecutionVertex]] = execution_graph.execution_vertices_by_job_vertex
        max_parallelism = execution_graph.get_max_parallelism()
        for i in range(max_parallelism):
            for job_vertex_id in exec_vertices_by_job_vertex_id:
                exec_vertices = exec_vertices_by_job_vertex_id[job_vertex_id]
                if i <= len(exec_vertices):
                    # current job vertex assigned
                    continue

                exec_vertex = exec_vertices[i]
                assert exec_vertex.resources is not None
                node = self._find_matched_node(nodes, exec_vertex.resources)
                assert exec_vertex.execution_vertex_id not in res
                res[exec_vertex.execution_vertex_id] = node

        return res
