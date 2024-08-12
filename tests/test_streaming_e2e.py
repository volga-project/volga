import string
import time
import unittest
from pathlib import Path
import random

import ray
import yaml

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.aggregate_function import AggregationType
from volga.streaming.api.function.function import SinkToCacheListFunction, SinkToCacheDictFunction
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
        sink_function = SinkToCacheListFunction(sink_cache)
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
        res = ray.get(sink_cache.get_list.remote())

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
        dict_size = 20
        num_gen_per_word = 10000
        word_length = 32

        dictionary = [''.join(random.choices(string.ascii_letters, k=word_length)) for _ in range(dict_size)]

        num_gen_left_per_word = {i: num_gen_per_word for i in range(len(dictionary))}

        # TODO make generator
        gen_words = []
        # randomly order
        while len(num_gen_left_per_word) != 0:
            index = random.randrange(len(num_gen_left_per_word))
            i = list(num_gen_left_per_word.keys())[index]
            gen_words.append(dictionary[i])
            num_gen_left_per_word[i] -= 1
            if num_gen_left_per_word[i] == 0:
                del num_gen_left_per_word[i]

        assert len(gen_words) == dict_size * num_gen_per_word
        _counts = {}
        for w in gen_words:
            _counts[w] = _counts.get(w, 0) + 1

        assert len(_counts) == dict_size
        for w in _counts:
            assert _counts[w] == num_gen_per_word

        sink_cache = SinkCacheActor.remote()

        sink_function = SinkToCacheDictFunction(sink_cache, key_value_extractor=(lambda e: (e[0], e[1])))

        # TODO set_parallelism > 1 fail assert
        source = self.ctx.from_collection(gen_words).set_parallelism(1)
        source.map(lambda wrd: (wrd, 1))\
            .key_by(lambda e: e[0])\
            .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1]))\
            .sink(sink_function)
            # .sink(lambda x: None)
        ctx.execute()

        counts = ray.get(sink_cache.get_dict.remote())

        assert len(counts) == dict_size
        for w in counts:
            assert counts[w] == num_gen_per_word

        print('assert ok')

    def test_parallel_collection_source(self):
        num_events = 1000
        parallelism = 10
        ins = [i for i in range(num_events)]
        s = self.ctx.from_collection(ins)
        s.set_parallelism(parallelism)
        sink_cache = SinkCacheActor.remote()
        sink_function = SinkToCacheListFunction(sink_cache)
        s.sink(sink_function)
        ctx.execute()
        res = ray.get(sink_cache.get_list.remote())
        assert sorted(res) == ins

if __name__ == '__main__':
    job_master = None
    try:
        ray.init(address='auto', ignore_reinit_error=True)
        job_config = yaml.safe_load(Path('../volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        t = TestStreamingJobE2E(ctx)
        # TODO should reset context on each call
        # t.test_join_streams()
        # t.test_window()
        # t.test_delayed_collection_source()
        # t.test_parallel_collection_source()

        t.test_wordcount()

        job_master = ctx.job_master
    finally:
        if job_master is not None:
            ray.get(job_master.destroy.remote())
        time.sleep(1)
        ray.shutdown()
