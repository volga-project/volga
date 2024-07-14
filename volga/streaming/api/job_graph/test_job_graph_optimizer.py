import unittest

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.job_graph.job_graph_optimizer import JobGraphOptimizer


class TestJobGraphOptimizer(unittest.TestCase):

    def test_simple_chain(self):
        ctx = StreamingContext()
        s1 = ctx.from_values('a', 'b', 'c').set_parallelism(2)\
            .map(lambda x: f'map_{x}')\
            .filter(lambda x: x is not None)

        s2 = ctx.from_values('a', 'b', 'c').map(lambda x: f'map_{x}')

        j = s1.key_by(lambda x: x).join(s2.key_by(lambda x: x))

        s = j.filter(lambda x: x).sink(lambda x: print(x))

        jgb = JobGraphBuilder(stream_sinks=[s])
        jg = jgb.build()
        print(jg.gen_digraph())
        optimizer = JobGraphOptimizer(jg)
        jg = optimizer.optimize()
        print(jg.job_vertices)
        # assert len(jg.job_vertices) == 1
        v = jg.job_vertices[0]

        print(jg.gen_digraph())

    def test_two_input_chain(self):
        pass

    def test_complex_chain(self):
        pass


if __name__ == '__main__':
    t = TestJobGraphOptimizer()
    t.test_simple_chain()