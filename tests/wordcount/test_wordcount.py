import random
import string
import unittest

import ray
import yaml
from pathlib import Path

from tests.wordcount.source import WordCountSource
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.function import SinkToCacheFunction
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor


class TestWordCount(unittest.TestCase):

    def test(self):
        job_config = yaml.safe_load(Path('../../volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)

        dict_size = 20
        count_per_word = 10000
        word_length = 32
        num_msgs_per_split = 1000

        dictionary = [''.join(random.choices(string.ascii_letters, k=word_length)) for _ in range(dict_size)]

        ray.init(address='auto', ignore_reinit_error=True)
        sink_cache = SinkCacheActor.remote()

        # TODO # we do not want to sink on each update, only the last one, make custom sink_function
        sink_function = SinkToCacheFunction(sink_cache)

        # TODO set_parallelism > 1 fail assert
        source = WordCountSource(
            streaming_context=ctx,
            parallelism=1,
            count_per_word=count_per_word,
            num_msgs_per_split=num_msgs_per_split,
            dictionary=dictionary
        )
        source.map(lambda wrd: (wrd, 1)) \
            .key_by(lambda e: e[0]) \
            .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1])) \
            .sink(sink_function)
        ctx.execute()

        res = ray.get(sink_cache.get_values.remote())
        counts = {}
        for (word, count) in res:
            counts[word] = max(counts.get(word, count), count)

        assert len(counts) == dict_size
        for w in counts:
            assert counts[w] == count_per_word

        print('assert ok')
        ray.shutdown()


if __name__ == '__main__':
    t = TestWordCount()
    t.test()