import logging
import unittest

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.job_graph.job_graph import JobGraph
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.partition.partition import RoundRobinPartition
from volga.streaming.runtime.core.execution_graph.execution_graph import ExecutionGraph

logger = logging.getLogger(__name__)


class TestExecutionGraph(unittest.TestCase):

    def test_from_job_graph(self):
        jg = self._build_sample_job_graph()
        eg = ExecutionGraph.from_job_graph(jg)
        # print_digraph(jg.gen_digraph(), name='job_graph.png')
        # print_digraph(eg.gen_digraph(), name='execution_graph.png')
        assert len(eg.execution_edges) == 8
        assert len(eg.execution_vertices_by_id.values()) == 6
        assert isinstance(eg.execution_vertices_by_id['2_1'].output_edges[0].partition, RoundRobinPartition)


    def _build_sample_job_graph(self) -> JobGraph:
        ctx = StreamingContext()
        sink = ctx.from_values('a', 'b', 'c') \
            .set_parallelism(2)\
            .map(lambda x: x)\
            .set_parallelism(2)\
            .sink(lambda x: logger.info(x))
        jgb = JobGraphBuilder(stream_sinks=[sink])
        return jgb.build()


if __name__ == '__main__':
    t = TestExecutionGraph()
    t.test_from_job_graph()