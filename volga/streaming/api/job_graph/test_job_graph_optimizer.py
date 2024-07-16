import unittest

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.job_graph.job_graph_optimizer import JobGraphOptimizer
from volga.streaming.api.operators.chained import ChainedSourceOperator, ChainedTwoInputOperator
from volga.streaming.api.operators.operators import FilterOperator, SourceOperator, SinkOperator, MapOperator, \
    KeyByOperator, JoinOperator
from volga.streaming.api.partition.partition import RoundRobinPartition, KeyPartition


class TestJobGraphOptimizer(unittest.TestCase):

    def test_simple_chain(self):
        print('test_simple_chain')
        ctx = StreamingContext()
        s = ctx.from_values('a', 'b', 'c').map(lambda x: f'map_{x}').filter(lambda x: True).sink(lambda x: print(x))
        jgb = JobGraphBuilder(stream_sinks=[s])
        jg = jgb.build()
        optimizer = JobGraphOptimizer(jg)
        jg = optimizer.optimize()
        print(jg.gen_digraph())
        vertices = jg.job_vertices
        edges = jg.job_edges
        assert len(vertices) == 1
        assert len(edges) == 0
        assert isinstance(vertices[0].stream_operator, ChainedSourceOperator)
        assert isinstance(vertices[0].stream_operator.operators[0], SourceOperator)
        assert isinstance(vertices[0].stream_operator.operators[1], MapOperator)
        assert isinstance(vertices[0].stream_operator.operators[2], FilterOperator)
        assert isinstance(vertices[0].stream_operator.operators[3], SinkOperator)

        print('assert ok')

    def test_complex_chain(self):
        print('test_complex_chain')
        ctx = StreamingContext()
        s1 = ctx.from_values('a', 'b', 'c').set_parallelism(2)\
            .map(lambda x: f'map_{x}')\
            .filter(lambda x: True)

        s2 = ctx.from_values('a', 'b', 'c').map(lambda x: f'map_{x}')

        j = s1.key_by(lambda x: x).join(s2.key_by(lambda x: x)).filter(lambda x: True)

        s3 = ctx.from_values('a', 'b', 'c')

        s = j.key_by(lambda x: x).join(s3.key_by(lambda x: x)).sink(lambda x: print(x))

        jgb = JobGraphBuilder(stream_sinks=[s])
        jg = jgb.build()
        optimizer = JobGraphOptimizer(jg)
        jg = optimizer.optimize()
        print(jg.gen_digraph())
        vertices = jg.job_vertices
        edges = jg.job_edges
        assert len(vertices) == 5
        assert len(edges) == 4

        assert isinstance(vertices[0].stream_operator, ChainedSourceOperator)

        assert isinstance(vertices[1].stream_operator, ChainedTwoInputOperator)
        assert isinstance(vertices[1].stream_operator.operators[0], JoinOperator)
        assert isinstance(vertices[1].stream_operator.operators[1], FilterOperator)
        assert isinstance(vertices[1].stream_operator.operators[2], KeyByOperator)

        assert isinstance(vertices[2].stream_operator, ChainedTwoInputOperator)
        assert isinstance(vertices[2].stream_operator.operators[0], JoinOperator)
        assert isinstance(vertices[2].stream_operator.operators[1], SinkOperator)

        assert isinstance(edges[0].partition, KeyPartition)
        assert isinstance(edges[1].partition, KeyPartition)
        assert isinstance(edges[2].partition, KeyPartition)
        assert isinstance(edges[3].partition, KeyPartition)


    def test_branching(self):
        print('test_branching')
        ctx = StreamingContext()
        s = ctx.from_values('a', 'b', 'c').filter(lambda x: True)
        sink1 = s.sink(lambda x: print(x))
        sink2 = s.sink(lambda x: print(x))
        jgb = JobGraphBuilder(stream_sinks=[sink1, sink2])
        jg = jgb.build()
        optimizer = JobGraphOptimizer(jg)
        jg = optimizer.optimize()
        print(jg.gen_digraph())
        vertices = jg.job_vertices
        edges = jg.job_edges
        assert len(vertices) == 3
        assert len(edges) == 2
        assert isinstance(vertices[0].stream_operator, ChainedSourceOperator)
        assert isinstance(vertices[0].stream_operator.operators[0], SourceOperator)
        assert isinstance(vertices[0].stream_operator.operators[1], FilterOperator)
        assert isinstance(vertices[1].stream_operator, SinkOperator)
        assert isinstance(vertices[2].stream_operator, SinkOperator)
        assert isinstance(edges[0].partition, RoundRobinPartition)
        assert isinstance(edges[1].partition, RoundRobinPartition)

        print('assert ok')



if __name__ == '__main__':
    t = TestJobGraphOptimizer()
    t.test_simple_chain()
    t.test_complex_chain()
    t.test_branching()
