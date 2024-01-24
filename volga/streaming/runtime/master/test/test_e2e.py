import time
import unittest
from pathlib import Path

import ray
import yaml

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.function import SinkToCacheFunction
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor


class TestE2E(unittest.TestCase):

    def __init__(self, ctx: StreamingContext):
        super().__init__()
        self.ctx = ctx

    def test_sample_stream(self):
        def map_func(x):
            import random
            odd = random.randint(0, 1)
            if odd == 1:
                return (x, 1)
            else:
                return (x, 0)

        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheFunction(sink_cache)
        source = self.ctx.from_collection([f'a{i}' for i in range(10)])
        # source = ctx.from_timed_collection([f'a{i}' for i in range(10)], 1)
        s = source.map(map_func) \
            .key_by(lambda x: x[1]) \
            .reduce(lambda x, y: f'{x}_{y}')

        s.sink(lambda x: print(x))
        s.sink(sink_function)

        ctx.submit()
        # time.sleep(5)
        # res = ray.get(sink_cache.get_values.remote())
        # print(res)

    def test_join_streams(self):

        source1 = self.ctx.from_collection([(i, f'a{i}') for i in range(30)])
        source2 = self.ctx.from_collection([(i, f'b{i}') for i in range(30)])

        # sink_cache = SinkCacheActor.remote()
        # sink_function = SinkToCacheFunction(sink_cache)

        s = source1.join(source2)\
            .where_key(lambda x: x[0])\
            .equal_to(lambda x: x[0])\
            .with_func(lambda x, y: (x, y)) \
            .filter(lambda x: x[0] != None and x[1] != None)

        # s.sink(sink_function)
        s.sink(lambda x: print(x))

        ctx.submit()

        # time.sleep(5)
        # res = ray.get(sink_cache.get_values.remote())
        # print(res)

    def test_parallelism(self):
        source1 = self.ctx.from_collection([(i, f'a{i}') for i in range(4)])
        s = source1.map(lambda x: f'map1={x}')
        s.set_parallelism(2)
        s = s.map(lambda x: f'map2={x}')
        s.set_parallelism(1)
        s.sink(lambda x: print(x)).set_parallelism(1)

        ctx.submit()


if __name__ == '__main__':
    job_master = None
    try:
        ray.init(address='auto')
        job_config = yaml.safe_load(Path('../../sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        t = TestE2E(ctx)
        # TODO should reset context on each call
        t.test_sample_stream()
        # t.test_parallelism()
        # t.test_join_streams()

        job_master = ctx.job_master
        time.sleep(1000)
    finally:
        if job_master != None:
            ray.get(job_master.destroy.remote())
        time.sleep(5)
        ray.shutdown()