import logging
import unittest

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.job_graph.job_graph import JobGraph, JobEdge, JobVertex, VertexType
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.partition.partition import ForwardPartition, KeyPartition

logger = logging.getLogger(__name__)


class TestJobGraph(unittest.TestCase):

    def test_data_sync_job_graph(self):
        jg = self._build_data_sync_job_graph()
        vertices = jg.job_vertices
        edges = jg.job_edges
        assert len(vertices) == 2
        assert len(edges) == 1

        je: JobEdge = edges[0]
        assert isinstance(je.partition, ForwardPartition)

        sink_vertex: JobVertex = vertices[1]
        source_vertex: JobVertex = vertices[0]
        assert sink_vertex.vertex_type == VertexType.SINK
        assert source_vertex.vertex_type == VertexType.SOURCE

    def test_key_by_job_graph(self):
        jg = self._build_key_by_job_graph()
        vertices = jg.job_vertices
        edges = jg.job_edges
        assert len(vertices) == 3
        assert len(edges) == 2

        source_vertex: JobVertex = vertices[0]
        map_vertex: JobVertex = vertices[1]
        sink_vertex: JobVertex = vertices[2]
        assert sink_vertex.vertex_type == VertexType.SINK
        assert map_vertex.vertex_type == VertexType.PROCESS
        assert source_vertex.vertex_type == VertexType.SOURCE

        key_by_to_sink: JobEdge = edges[0]
        source_to_key_by: JobEdge = edges[1]
        assert isinstance(key_by_to_sink.partition, KeyPartition)
        assert isinstance(source_to_key_by.partition, ForwardPartition)

        # print_digraph(jg.gen_digraph())


    def _build_data_sync_job_graph(self) -> JobGraph:
        ctx = StreamingContext()
        sink = ctx.from_values('a', 'b', 'c').sink(lambda x: logger.info(x))
        jgb = JobGraphBuilder(stream_sinks=[sink])
        return jgb.build()

    def _build_key_by_job_graph(self) -> JobGraph:
        ctx = StreamingContext()
        sink = ctx.from_values('1', '2', '3', '4').key_by(lambda x: x).sink(lambda x: logger.info(x))
        jgb = JobGraphBuilder(stream_sinks=[sink])
        return jgb.build()


if __name__ == '__main__':
    t = TestJobGraph()
    t.test_data_sync_job_graph()
    t.test_key_by_job_graph()
