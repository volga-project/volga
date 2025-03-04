import logging
from typing import Dict, List, Optional

from ray.actor import ActorHandle

from volga.streaming.api.job_graph.job_graph import JobGraph, JobVertex, VertexType
from volga.streaming.api.operators.operators import StreamOperator
from volga.streaming.api.partition.partition import RoundRobinPartition, Partition, ForwardPartition

from volga.streaming.common.config.resource_config import ResourceConfig
from volga.common.ray.resource_manager import \
    Resources, RESOURCE_KEY_CPU, RESOURCE_KEY_GPU, RESOURCE_KEY_MEM
from volga.streaming.runtime.network.channel import Channel

logger = logging.getLogger(__name__)


class ExecutionEdge:

    def __init__(
        self,
        source_execution_vertex: 'ExecutionVertex',
        target_execution_vertex: 'ExecutionVertex',
        partition: Partition
    ):
        self.source_execution_vertex = source_execution_vertex
        self.target_execution_vertex = target_execution_vertex
        self.partition = partition
        self.id = self._gen_id()
        self.channel = None

    def _gen_id(self):
        return f'{self.source_execution_vertex.execution_vertex_id}-{self.target_execution_vertex.execution_vertex_id}'

    def set_channel(self, channel: Channel):
        self.channel = channel


class ExecutionVertex:

    def __init__(
        self,
        job_name: str,
        job_vertex: JobVertex,
        execution_vertex_index: int, # sub index based on parallelism of job vertex operator
        parallelism: int,
        stream_operator: StreamOperator,
        job_config: Optional[Dict] = None,
        resources: Optional[Resources] = None
    ):
        self.job_name = job_name
        self.job_vertex = job_vertex
        self.execution_vertex_index = execution_vertex_index
        self.execution_vertex_id = self._gen_id()
        self.parallelism = parallelism
        self.stream_operator = stream_operator
        self.resources = resources
        self.job_config = job_config
        self.input_edges: List[ExecutionEdge] = []
        self.output_edges: List[ExecutionEdge] = []
        self.worker = None
        self.worker_node_info = None

    def __repr__(self):
        return f'{self.execution_vertex_id}, {self.job_vertex.stream_operator.__class__.__name__}'

    def _gen_id(self) -> str:
        return f'{self.job_vertex.vertex_id}_{self.execution_vertex_index}'

    def get_output_channels(self) -> List[Channel]:
        return [e.channel for e in self.output_edges]

    def get_input_channels(self) -> List[Channel]:
        return [e.channel for e in self.input_edges]

    def set_worker(self, worker: ActorHandle):
        self.worker = worker

    def set_resources(self, resources: Resources):
        self.resources = resources

    def set_worker_node_info(self, info: 'WorkerNodeInfo'):
        self.worker_node_info = info


class ExecutionGraph:

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.execution_vertices_by_id: Dict[str, ExecutionVertex] = {}
        self.execution_edges: List[ExecutionEdge] = []

        # parallelism groups for same operator
        self.execution_vertices_by_job_vertex: Dict[int, List[ExecutionVertex]] = {}

        self._max_parallelism = None

    @classmethod
    def from_job_graph(cls, job_graph: JobGraph) -> 'ExecutionGraph':

        execution_graph = ExecutionGraph(job_graph.job_name)
        execution_graph._max_parallelism = job_graph.get_max_parallelism()

        # create exec vertices
        for job_vertex in job_graph.job_vertices:
            for i in range(job_vertex.parallelism):
                execution_vertex = ExecutionVertex(
                    job_name=job_graph.job_name,
                    job_vertex = job_vertex,
                    execution_vertex_index=i,
                    parallelism=job_vertex.parallelism,
                    stream_operator=job_vertex.stream_operator
                )

                if job_vertex.vertex_id in execution_graph.execution_vertices_by_job_vertex:
                    execution_graph.execution_vertices_by_job_vertex[job_vertex.vertex_id].append(execution_vertex)
                else:
                    execution_graph.execution_vertices_by_job_vertex[job_vertex.vertex_id] = [execution_vertex]

                execution_graph.execution_vertices_by_id[execution_vertex.execution_vertex_id] = execution_vertex

        # create exec edges
        for job_edge in job_graph.job_edges:
            source_job_vertex_id = job_edge.source_vertex_id
            target_job_vertex_id = job_edge.target_vertex_id

            for source_exec_vertex in execution_graph.execution_vertices_by_job_vertex[source_job_vertex_id]:

                target_exec_vertices = execution_graph.execution_vertices_by_job_vertex[target_job_vertex_id]

                for target_exec_vertex in target_exec_vertices:
                    partition = job_edge.partition
                    # update partition
                    if isinstance(partition, ForwardPartition) and len(target_exec_vertices) > 1:
                        partition = RoundRobinPartition()

                    edge = ExecutionEdge(
                        source_execution_vertex=source_exec_vertex,
                        target_execution_vertex=target_exec_vertex,
                        partition=partition
                    )
                    source_exec_vertex.output_edges.append(edge)
                    target_exec_vertex.input_edges.append(edge)
                    execution_graph.execution_edges.append(edge)

        return execution_graph

    def gen_digraph(self):
        try:
            import pygraphviz as pgv
        except Exception:
            return "GraphViz is not installed. To enable ExecutionGraph visualization, please install GraphViz and pygraphviz"
        G = pgv.AGraph()
        for v in self.execution_vertices_by_id.values():
            G.add_node(v.execution_vertex_id, label=f'{v.stream_operator.get_name()}_{v.execution_vertex_id} p={v.parallelism}')

        for e in self.execution_edges:
            G.add_edge(e.source_execution_vertex.execution_vertex_id, e.target_execution_vertex.execution_vertex_id, label=e.partition.__class__.__name__)

        return G

    def get_source_vertices(self) -> List[ExecutionVertex]:
        return [v for v in self.execution_vertices_by_id.values() if v.job_vertex.vertex_type == VertexType.SOURCE]

    def get_non_source_vertices(self) -> List[ExecutionVertex]:
        return [v for v in self.execution_vertices_by_id.values() if v.job_vertex.vertex_type != VertexType.SOURCE]

    def get_max_parallelism(self):
        if self._max_parallelism is None:
            raise RuntimeError('Execution graph is not inited')
        return self._max_parallelism

    def set_resources(self, resource_config: ResourceConfig):
        for execution_vertex in self.execution_vertices_by_id.values():
            op_name = execution_vertex.job_vertex.get_name()
            resources = Resources.from_dict(resource_config.default_worker_resources)

            # update resource per op-type
            if resource_config.proposed_operator_resources is not None:
                for _op_name in resource_config.proposed_operator_resources:
                    if op_name == _op_name:
                        logger.info(f'Using custom resource for {op_name}')
                        _resources_dict = resource_config.proposed_operator_resources[_op_name]
                        if RESOURCE_KEY_CPU in _resources_dict:
                            resources.num_cpus = float(_resources_dict[RESOURCE_KEY_CPU])
                        if RESOURCE_KEY_GPU in _resources_dict:
                            resources.num_gpus = float(_resources_dict[RESOURCE_KEY_GPU])
                        if RESOURCE_KEY_MEM in _resources_dict:
                            resources.memory = _resources_dict[RESOURCE_KEY_MEM]

            execution_vertex.set_resources(resources)
        logger.info(f'Set execution graph resources')



