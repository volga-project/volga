from typing import Dict, List

from volga.streaming.api.job_graph.job_graph import JobGraph, JobVertex, JobEdge
from volga.streaming.api.operator.chained import new_chained_operator
from volga.streaming.api.operator.operator import StreamOperator
from volga.streaming.api.partition.partition import ForwardPartition, Partition, RoundRobinPartition


class JobGraphOptimizer:

    def __init__(self, job_graph: JobGraph):
        self.job_graph = job_graph
        self._visited = set()

        # vertex id -> vertex
        self._vertex_map: Dict[int, JobVertex] = {v.vertex_id: v for v in self.job_graph.job_vertices}
        self._output_edges_map: Dict[JobVertex, List[JobEdge]] = {
            self._vertex_map[vertex_id]: self.job_graph.get_vertex_output_edges(vertex_id) for vertex_id in self._vertex_map
        }

        # vertex id -> chained vertices
        self._chain_set: Dict[int, List[JobVertex]] = {}

        # head vertex id -> the edges to another ChainedOperator
        self._tail_edges_map:  Dict[int, List[JobEdge]] = {}

    def optimize(self) -> JobGraph:
        # reset next operators
        for vertex in self.job_graph.job_vertices:
            vertex.stream_operator.set_next_operators([])

        for source_vertex in self.job_graph.get_source_vertices():
            self.add_vertex_to_chain_set(source_vertex.vertex_id, source_vertex)
            self.divide_vertices(source_vertex.vertex_id, source_vertex)

        return JobGraph(
            self.job_graph.job_name,
            self.job_graph.job_config,
            self.merge_vertices(),
            self.reset_edges()
        )

    # Divide the original DAG into multiple operator groups, each group's operators will be chained
    # as one ChainedOperator.
    #
    # @param headVertexId The head vertex id of this ChainedOperator.
    # @param srcVertex The source vertex of this edge.
    def divide_vertices(self, head_vertex_id: int, src_vertex: JobVertex):
        output_edges = self._output_edges_map[src_vertex]
        for output_edge in output_edges:
            target_id = output_edge.target_vertex_id
            target_vertex = self._vertex_map[target_id]
            src_vertex.stream_operator.add_next_operator(target_vertex.stream_operator)
            if self.can_chain(src_vertex, target_vertex, output_edge):
                self.add_vertex_to_chain_set(head_vertex_id, target_vertex)
                self.divide_vertices(head_vertex_id, target_vertex)
            else:
                self.add_tail_edge(
                    head_vertex_id,
                    output_edge,
                    src_vertex.stream_operator.id,
                    output_edge.target_operator_id
                )
                if target_vertex not in self._visited:
                    self._visited.add(target_vertex)
                    self.add_vertex_to_chain_set(target_id, target_vertex)
                    self.divide_vertices(target_id, target_vertex)
                else:
                    self._visited.add(target_vertex)

    def add_vertex_to_chain_set(self, root_id: int, target_vertex: JobVertex):
        if root_id in self._chain_set:
            self._chain_set[root_id].append(target_vertex)
        else:
            self._chain_set[root_id] = [target_vertex]

    def add_tail_edge(self, head_vertex_id: int, output_edge: JobEdge, src_operator_id: int, target_operator_id: int):
        new_edge = JobEdge(
            output_edge.source_vertex_id,
            output_edge.target_vertex_id,
            output_edge.partition,
            src_operator_id,
            target_operator_id
        )
        new_edge.is_join_right_edge = output_edge.is_join_right_edge
        if head_vertex_id in self._tail_edges_map:
            self._tail_edges_map[head_vertex_id].append(new_edge)
        else:
            self._tail_edges_map[head_vertex_id] = [new_edge]

    def can_chain(self, src_vertex: JobVertex, target_vertex: JobVertex, edge: JobEdge) -> bool:
        if src_vertex.parallelism != target_vertex.parallelism:
            return False

        if len(self.job_graph.get_vertex_input_edges(target_vertex.vertex_id)) > 1:
            return False

        return isinstance(edge.partition, ForwardPartition)

    def reset_edges(self) -> List[JobEdge]:
        new_job_edges = []
        for head_vertex_id in self._tail_edges_map:
            tail_edges = self._tail_edges_map[head_vertex_id]
            for tail_edge in tail_edges:
                partition = _change_partition(tail_edge.partition)
                new_edge = JobEdge(
                    head_vertex_id,
                    tail_edge.target_vertex_id,
                    partition,
                    tail_edge.source_operator_id,
                    tail_edge.target_operator_id
                )
                new_edge.is_join_right_edge = tail_edge.is_join_right_edge
                new_job_edges.append(new_edge)
        new_job_edges.sort(key=lambda e: e.is_join_right_edge)
        return new_job_edges

    def merge_vertices(self) -> List[JobVertex]:
        new_job_vertices = []
        for head_vertex_id in self._chain_set:
            tree_list = self._chain_set[head_vertex_id]
            head_vertex = tree_list[0]
            if len(tree_list) == 1:
                # no chain
                merged_vertex = head_vertex
            else:
                operators = [self._vertex_map[v.vertex_id].stream_operator for v in tree_list]
                # TODO chain resource_configs and op_configs
                operator = new_chained_operator(operators)
                print(type(operator))
                assert isinstance(operator, StreamOperator)
                merged_vertex = JobVertex(
                    head_vertex.vertex_id,
                    head_vertex.parallelism,
                    head_vertex.vertex_type,
                    operator
                )
            new_job_vertices.append(merged_vertex)

        return new_job_vertices


def _change_partition(partition: Partition):
    if isinstance(partition, ForwardPartition):
        return RoundRobinPartition()
    else:
        return partition
