import time
import unittest
from pathlib import Path

import ray
import yaml

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.aggregate_function import AggregationType
from volga.streaming.api.function.function import SinkToCacheFunction
from volga.streaming.api.operator.timestamp_assigner import EventTimeAssigner
from volga.streaming.api.operator.window_operator import SlidingWindowConfig
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor

from decimal import Decimal

class TestE2E(unittest.TestCase):

    def __init__(self, ctx: StreamingContext):
        super().__init__()
        self.ctx = ctx

    def test_join_streams(self):

        s1_num_events = 3000
        s2_num_events = 1005

        source1 = self.ctx.from_collection([(i, f'a{i}') for i in range(s1_num_events)])
        source2 = self.ctx.from_collection([(i + 1, f'b{i + 1}') for i in range(s2_num_events)])

        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheFunction(sink_cache)

        s = source1.join(source2)\
            .where_key(lambda x: x[0])\
            .equal_to(lambda x: x[0])\
            .with_func(lambda x, y: (x, y)) \
            .set_parallelism(4) \
            .filter(lambda x: x[0] != None and x[1] != None) \
            .set_parallelism(1) \
            .map(lambda x: (x[0][0], x[0][1], x[1][1]))

        s.sink(sink_function)
        s.sink(lambda x: print(x))

        ctx.submit()

        time.sleep(5)
        res = ray.get(sink_cache.get_values.remote())
        print(len(res))
        print(res[0], res[-1])

        # find missing
        missing = []
        for i in range(s2_num_events):
            found = False
            for e in res:
                if i == e[0]:
                    found = True
                    break
            if not found:
                missing.append(i)

        print(f'Missing {len(missing)} items')
        # print(f'{missing}')

    def test_window(self):
        s = self.ctx.from_collection([('k', 1), ('k', 2), ('k', 3), ('k', 4)])
        s = s.timestamp_assigner(EventTimeAssigner(lambda e: Decimal(time.time())))
        s.key_by(lambda e: e[0]).multi_window_agg([
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.COUNT,
                agg_on=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.SUM,
                agg_on=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.AVG,
                agg_on=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.MAX,
                agg_on=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.MIN,
                agg_on=(lambda e: e[1]),
            )
        ]).sink(print)

        ctx.submit()


if __name__ == '__main__':
    job_master = None
    try:
        ray.init(address='auto')
        job_config = yaml.safe_load(Path('../../sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        t = TestE2E(ctx)
        # TODO should reset context on each call
        # t.test_join_streams()
        t.test_window()

        job_master = ctx.job_master
        time.sleep(1000)
    finally:
        if job_master != None:
            ray.get(job_master.destroy.remote())
        time.sleep(1)
        ray.shutdown()