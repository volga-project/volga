import random
import string
import time
import unittest

import ray
import yaml
from pathlib import Path

from volga.streaming.runtime.network.testing_utils import RAY_ADDR, REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV
from volga.streaming.runtime.sources.wordcount.source import WordCountSource, WordCountSourceSplitEnumerator
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.function import SinkToCacheDictFunction
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor
from volga.streaming.runtime.sources.source_splits_manager import SourceSplitType


class TestWordCount(unittest.TestCase):

    # TODO move this to wordcount source
    def test_word_count_source_split_enumerator(self):
        count_per_word = 5
        num_msgs_per_split = 2
        dictionary = ['a', 'b', 'c', 'd', 'e']

        e = WordCountSourceSplitEnumerator(
            count_per_word=count_per_word,
            num_msgs_per_split=num_msgs_per_split,
            dictionary=dictionary
        )
        splits = []
        while True:
            split = e.poll_next_split(task_id=1)
            splits.append(split)
            if split.type == SourceSplitType.END_OF_INPUT:
                break
        counts = {}

        words_left = len(dictionary) * count_per_word
        for split in splits:
            if split.type != SourceSplitType.END_OF_INPUT:
                num_words = 0
                for word in split.data:
                    count = split.data[word]
                    num_words += count
                    if word in counts:
                        counts[word] += count
                    else:
                        counts[word] = count
                assert num_words == min(words_left, num_msgs_per_split)
                words_left -= num_words

        assert len(counts) == len(dictionary)
        for w in counts:
            assert counts[w] == count_per_word

        print('assert ok')

    def test(self):
        job_config = yaml.safe_load(Path('../volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)

        dict_size = 10
        count_per_word = 10000000
        word_length = 32
        num_msgs_per_split = 100000

        dictionary = [''.join(random.choices(string.ascii_letters, k=word_length)) for _ in range(dict_size)]

        ray.init(address=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, ignore_reinit_error=True)
        # ray.init(address='auto', ignore_reinit_error=True)
        sink_cache = SinkCacheActor.remote()

        sink_function = SinkToCacheDictFunction(sink_cache, key_value_extractor=(lambda e: (e[0], e[1])))

        source = WordCountSource(
            streaming_context=ctx,
            parallelism=8,
            count_per_word=count_per_word,
            num_msgs_per_split=num_msgs_per_split,
            dictionary=dictionary
        )
        s = source.map(lambda wrd: (wrd, 1)) \
            .key_by(lambda e: e[0]) \
            .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1]))
        s1 = s.sink(sink_function)
        # s2 = s.sink(lambda e: None)
        start = time.time()
        ctx.execute()
        end = time.time()

        counts = ray.get(sink_cache.get_dict.remote())
        print(counts)
        assert len(counts) == dict_size
        total = 0
        for w in counts:
            total += counts[w]

        exec_time = end - start
        estimate_throughput = total/exec_time

        print(f'Exec time: {exec_time}, Throughput Est: {estimate_throughput}, Total: {total}, expected: {count_per_word * dict_size}, diff: {count_per_word * dict_size - total}')

        for w in counts:
            assert counts[w] == count_per_word

        print('assert ok')
        ray.shutdown()


if __name__ == '__main__':
    t = TestWordCount()
    # t.test_word_count_source_split_enumerator()
    t.test()
