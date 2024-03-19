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


class TestStreamingJobE2E(unittest.TestCase):

    def __init__(self, ctx: StreamingContext):
        super().__init__()
        self.ctx = ctx

    def test_join_streams(self):

        # TODO increasing this 10x prevents sources from reporting finish, why?
        s1_num_events = 30000
        s2_num_events = 10000

        source1 = self.ctx.from_collection([(i, f'a{i}') for i in range(s1_num_events)])
        source2 = self.ctx.from_collection([(i + 1, f'b{i + 1}') for i in range(s2_num_events)])

        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheFunction(sink_cache)

        s = source1.key_by(lambda x: x[0])\
            .join(source2.key_by(lambda x: x[0]))\
            .with_func(lambda x, y: (x, y)) \
            .set_parallelism(10) \
            .map(lambda x: (x[0][0], x[0][1], x[1][1])) \
            .set_parallelism(1)

        s.sink(sink_function)
        s.sink(lambda x: print(x) if x[0]%1000 == 0 else None)
        # s.sink(print)
        ctx.execute()
        res = ray.get(sink_cache.get_values.remote())
        print(len(res))
        assert len(res) == min(s1_num_events, s2_num_events)
        print('assert ok')

    def test_window(self):
        s = self.ctx.from_collection([
            *[('k1', i) for i in range(100)],
            *[('k2', i) for i in range(100)],
        ])
        s = s.timestamp_assigner(EventTimeAssigner(lambda e: Decimal(time.time())))
        s.key_by(lambda e: e[0]).multi_window_agg([
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.COUNT,
                agg_on_func=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.SUM,
                agg_on_func=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.AVG,
                agg_on_func=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.MAX,
                agg_on_func=(lambda e: e[1]),
            ),
            SlidingWindowConfig(
                duration='1m',
                agg_type=AggregationType.MIN,
                agg_on_func=(lambda e: e[1]),
            )
        ]).sink(print)

        ctx.execute()
        # TODO assert


if __name__ == '__main__':
    job_master = None
    try:
        ray.init(address='auto')
        job_config = yaml.safe_load(Path('../../sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        t = TestStreamingJobE2E(ctx)
        # TODO should reset context on each call
        # t.test_join_streams()
        t.test_window()

        job_master = ctx.job_master
    finally:
        if job_master is not None:
            ray.get(job_master.destroy.remote())
        time.sleep(1)
        ray.shutdown()
