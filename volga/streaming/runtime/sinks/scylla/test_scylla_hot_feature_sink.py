import time
import unittest
from pathlib import Path

import ray
import yaml

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.runtime.sinks.scylla.scylla_hot_feature_sink_function import ScyllaHotFeatureSinkFunction


class TestScyllaHotFeatureSinkFunction(unittest.TestCase):

    def test(self):
        ray.init(address='auto', ignore_reinit_error=True)
        job_config = yaml.safe_load(
            Path('/Users/anov/IdeaProjects/volga/volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        num_events = 100000
        ins = [{'key': f'key_{i}', 'value': f'value_{i}'} for i in range(num_events)]
        s = ctx.from_collection(ins)

        # TODO if parallelism > 1 we also need to add key_by operator to make sure sink workers do not share keys -
        # this will avoid data races when writing to hot storage
        parallelism = 1
        s.set_parallelism(parallelism)

        def _extractor(e):
            return {'key': e['key']}, {'value': e['value']}

        sink_function = ScyllaHotFeatureSinkFunction(
            feature_name='feature1',
            key_value_extractor=_extractor,
        )
        s.sink(sink_function)
        ctx.execute()
        time.sleep(1)
        ray.shutdown()


if __name__ == '__main__':
    t = TestScyllaHotFeatureSinkFunction()
    t.test()