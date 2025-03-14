import json
import random
import string
import time
import unittest
from typing import Optional, Any, Tuple

import ray
import yaml
from pathlib import Path

from volga.streaming.runtime.network.network_config import DEFAULT_NETWORK_CONFIG

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.function import SinkToCacheDictFunction
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor
from volga.streaming.runtime.sources.wordcount.timed_source import WordCountSource

DEFAULT_DICT_SIZE = 100  # num of words in a dict
DEFAULT_SPLIT_SIZE = 1000000  # num of words in a source split


class TestWordCount(unittest.TestCase):

    def test_wordcount(
        self,
        parallelism: int = 1,
        word_length: int = 32,
        batch_size: int = 1000,
        run_for_s: int = 25,
        ray_addr: Optional[str] = None,
        runtime_env: Optional[Any] = None,
        run_assert: bool = False
    ) -> Tuple:
        job_config = yaml.safe_load(Path('/Users/anov/IdeaProjects/volga/volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)

        network_config = DEFAULT_NETWORK_CONFIG
        network_config.data_writer.batch_size = batch_size
        job_config['network_config'] = json.loads(network_config.json())

        dictionary = [''.join(random.choices(string.ascii_letters, k=word_length)) for _ in range(DEFAULT_DICT_SIZE)]

        ray.init(address=ray_addr, runtime_env=runtime_env, ignore_reinit_error=True)
        sink_cache = SinkCacheActor.remote()

        # source = WordCountSplitSource(
        #     streaming_context=ctx,
        #     parallelism=parallelism,
        #     dictionary=dictionary,
        #     split_size=DEFAULT_SPLIT_SIZE,
        #     run_for_s=run_for_s,
        #     # count_per_word=count_per_word,
        # )
        source = WordCountSource(
            streaming_context=ctx,
            dictionary=dictionary,
            run_for_s=run_for_s
        )
        source.set_parallelism(parallelism)
        s = source.map(lambda wrd: (wrd, 1)) \
            .key_by(lambda e: e[0]) \
            .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1]))
        s.sink(SinkToCacheDictFunction(sink_cache, key_value_extractor=(lambda e: (e[0], e[1]))))

        start = time.time()
        ctx.execute(timeout_s=(run_for_s + 5))
        end = time.time()
        job_master = ctx.job_master
        num_sent_per_source_worker = ray.get(job_master.get_num_sent_per_source_worker.remote())
        # merge all sent for the job
        num_sent = {}
        for source_vertex_id in num_sent_per_source_worker:
            _num_sent = num_sent_per_source_worker[source_vertex_id]
            for w in _num_sent:
                if w in num_sent:
                    num_sent[w] += _num_sent[w]
                else:
                    num_sent[w] = _num_sent[w]

        avg_throughput, latency_stats, hist_throughput, hist_latency = ray.get(job_master.get_final_perf_stats.remote())

        counts = ray.get(sink_cache.get_dict.remote())

        total_num_sent = 0
        for w in counts:
            total_num_sent += counts[w]

        run_duration = end - start
        estimated_throughput = total_num_sent/run_duration

        print(f'Finished in {run_duration}s \n'
              f'Avg Throughput: {avg_throughput} msg/s \n'
              f'Estimated Throughput: {estimated_throughput} msg/s \n'
              f'Latency: {latency_stats} \n')

        if run_assert:
            print(counts)
            print(num_sent)
            assert len(counts) == DEFAULT_DICT_SIZE
            for w in counts:
                assert counts[w] == num_sent[w]
            print('assert ok')

        ray.shutdown()

        return avg_throughput, latency_stats, total_num_sent, hist_throughput, hist_latency


if __name__ == '__main__':
    t = TestWordCount()
    t.test_wordcount(parallelism=1, word_length=1024, batch_size=1000, run_for_s=30, run_assert=True)
    # t.test_wordcount(parallelism=200, word_length=32, batch_size=1, run_for_s=20, ray_addr=RAY_ADDR, runtime_env=REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV, run_assert=False)
