import string
import time
import unittest
from pathlib import Path
import random

import ray
import yaml

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.aggregate_function import AggregationType
from volga.streaming.api.function.function import SinkToCacheFunction
from volga.streaming.api.operators.timestamp_assigner import EventTimeAssigner
from volga.streaming.api.operators.window_operator import SlidingWindowConfig
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor

from decimal import Decimal


class TestStreamingJobE2E(unittest.TestCase):

    def __init__(self, ctx: StreamingContext):
        super().__init__()
        self.ctx = ctx

    def test_join_streams(self):
        def dummy_join(a, b, k1, k2, how):

            def listify(t):
                return [listify(i) for i in t] if isinstance(t, (list, tuple)) else t

            d1 = {k1(e): e for e in a}
            d2 = {k2(e): e for e in b}
            res = []
            for k in d1:
                if k in d2:
                    res.append(how(d1[k], d2[k]))

            # TODO for some reason volga transforms tuples to lists, figure out why
            return listify(res)

        # TODO increasing this 10x fails ray memory ser (in SinkCacheActor), too big?
        s1_num_events = 100000
        s2_num_events = 100000
        s1 = [(i, f'a{i}') for i in range(s1_num_events)]
        s2 = [(i + 1, f'b{i + 1}') for i in range(s2_num_events)]

        source1 = self.ctx.from_collection(s1).set_parallelism(4)
        source2 = self.ctx.from_collection(s2).set_parallelism(4)

        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheFunction(sink_cache)
        k1 = lambda x: x[0]
        k2 = lambda x: x[0]
        how = lambda x, y: (x, y)
        s = source1.key_by(k1) \
            .join(source2.key_by(k2)) \
            .with_func(how)

        s.sink(sink_function)

        start_ts = time.time()
        ctx.execute()
        print(f'Finished in {time.time() - start_ts}s')
        res = ray.get(sink_cache.get_values.remote())

        expected = dummy_join(s1, s2, k1, k2, how)
        assert len(res) == len(expected)
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

    def test_wordcount(self):
        num_word_types = 20
        num_words_per_type = 10000
        word_length = 32

        word_types = [''.join(random.choices(string.ascii_letters, k=word_length)) for _ in range(num_word_types)]

        num_words_left_per_type = {i: num_words_per_type for i in range(len(word_types))}

        # TODO make generator
        words = []
        # randomly order
        while len(num_words_left_per_type) != 0:
            index = random.randrange(len(num_words_left_per_type))
            i = list(num_words_left_per_type.keys())[index]
            words.append(word_types[i])
            num_words_left_per_type[i] -= 1
            if num_words_left_per_type[i] == 0:
                del num_words_left_per_type[i]

        assert len(words) == num_word_types * num_words_per_type
        _counts = {}
        for w in words:
            _counts[w] = _counts.get(w, 0) + 1

        assert len(_counts) == num_word_types
        for w in _counts:
            assert _counts[w] == num_words_per_type

        sink_cache = SinkCacheActor.remote()

        # TODO # we do not want to sink on each update, only the last one, make custom sink_function
        sink_function = SinkToCacheFunction(sink_cache)

        # TODO set_parallelism > 1 fail assert
        source = self.ctx.from_collection(words).set_parallelism(1)
        source.map(lambda wrd: (wrd, 1))\
            .key_by(lambda e: e[0])\
            .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1]))\
            .sink(sink_function)
            # .sink(lambda x: None)
        ctx.execute()

        res = ray.get(sink_cache.get_values.remote())
        counts = {}
        for (word, count) in res:
            counts[word] = max(counts.get(word, count), count)

        assert len(counts) == num_word_types
        for w in counts:
            assert counts[w] == num_words_per_type

        print('assert ok')

    def test_parallel_collection_source(self):
        num_events = 1000
        parallelism = 10
        ins = [i for i in range(num_events)]
        s = self.ctx.from_collection(ins)
        s.set_parallelism(parallelism)
        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheFunction(sink_cache)
        s.sink(sink_function)
        ctx.execute()
        res = ray.get(sink_cache.get_values.remote())
        assert sorted(res) == ins

if __name__ == '__main__':
    job_master = None
    try:
        ray.init(address='auto', ignore_reinit_error=True)
        job_config = yaml.safe_load(Path('../volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        t = TestStreamingJobE2E(ctx)
        # TODO should reset context on each call
        t.test_join_streams()
        # t.test_window()
        # t.test_delayed_collection_source()
        # t.test_parallel_collection_source()

        # t.test_wordcount()

        job_master = ctx.job_master
    finally:
        if job_master is not None:
            ray.get(job_master.destroy.remote())
        time.sleep(1)
        ray.shutdown()
