import enum
import time
from typing import List, Dict, Optional, Any

from volga.streaming.api.operator.operator import StreamOperator
from volga.streaming.api.partition.partition import Partition


class JobEdge:
    def __init__(
        self,
        source_vertex_id: int,
        target_vertex_id: int,
        partition: Partition,
        source_operator_id: Optional[int] = None,
        target_operator_id: Optional[int] = None
    ):
        self.source_vertex_id = source_vertex_id
        self.target_vertex_id = target_vertex_id

        if source_operator_id is None:
            self.source_operator_id = source_vertex_id
        else:
            self.source_operator_id = source_operator_id

        if target_operator_id is None:
            self.target_operator_id = target_vertex_id
        else:
            self.target_operator_id = target_operator_id

        self.partition = partition
        self.is_join_right_edge = False


class VertexType(enum.Enum):
    SOURCE = 0  # data reader, 0 input, 1 output
    PROCESS = 1  # 1 input, 1 output
    SINK = 2  # data writer, 1 input, 0 output
    UNION = 3  # simply group all input elements, 2 inputs, 1 output,
    JOIN = 4  # group input elements with a specified method, 2 inputs, 1 output


class JobVertex:

    def __init__(
        self,
        vertex_id: int,
        parallelism: int,
        vertex_type: VertexType,
        stream_operator: StreamOperator,
    ):
        self.vertex_id = vertex_id
        self.parallelism = parallelism
        self.vertex_type = vertex_type
        self.stream_operator = stream_operator

        # set operator id
        self.stream_operator.id = vertex_id

    def get_name(self) -> str:
        return f'{self.vertex_id}_{self.stream_operator.__class__.__name__}'


class JobGraph:
    def __init__(
        self,
        job_name: Optional[str],
        job_config: Optional[Dict]
    ):
        if job_name is None:
            job_name = f'job_{int(time.time())}'
        self.job_name = job_name
        self.job_config = job_config
        self.job_vertices: List[JobVertex] = []
        self.job_edges: List[JobEdge] = []

    def add_vertex_if_not_exists(self, job_vertex: JobVertex):
        for vertex in self.job_vertices:
            if vertex.vertex_id == job_vertex.vertex_id:
                return
        self.job_vertices.append(job_vertex)

    def add_edge_if_not_exists(self, job_edge: JobEdge):
        for edge in self.job_edges:
            if edge.source_vertex_id == job_edge.source_vertex_id and \
                    edge.target_vertex_id == job_edge.target_vertex_id:
                return
        self.job_edges.append(job_edge)

    def get_vertex_output_edges(self, vertex_id: int) -> List[JobEdge]:
        return list(filter(lambda e: e.source_vertex_id == vertex_id, self.job_edges))

    def get_vertex_input_edges(self, vertex_id: int) -> List[JobEdge]:
        return list(filter(lambda e: e.target_vertex_id == vertex_id, self.job_edges))

    def get_source_vertices(self) -> List[JobVertex]:
        return list(filter(lambda v: v.vertex_type == VertexType.SOURCE, self.job_vertices))

    def gen_digraph(self):
        try:
            import pygraphviz as pgv
        except Exception:
            return "GraphViz is not installed. To enable JobGraph visualization, please install GraphViz and pygraphviz"
        G = pgv.AGraph()
        for jv in self.job_vertices:
            G.add_node(jv.vertex_id, label=f'{jv.stream_operator.__class__.__name__} p={jv.parallelism}')

        for je in self.job_edges:
            G.add_edge(je.source_vertex_id, je.target_vertex_id, label=je.partition.__class__.__name__)

        return G