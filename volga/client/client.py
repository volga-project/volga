import time
from typing import Dict, Optional

from ray.util.client import ray

from volga.data.api.dataset.dataset import Dataset
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.stream.data_stream import DataStream


class Client:

    def __init__(self):
        pass

    def materialize_offline(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]] = None):
        ctx = StreamingContext(job_config={
            'worker_config_template': {},
            'master_config': {
                'resource_config': {
                    'default_worker_resources': {
                        'CPU': '0.1'
                    }
                },
                'scheduler_config': {}
            }
        })
        pipeline = target.get_pipeline()

        # build stream
        # init sources
        for inp in pipeline.inputs:
            assert isinstance(inp, Dataset)
            if not inp.is_source():
                raise ValueError('Currently only source inputs are allowed')
            source_tag = None
            if source_tags is not None and inp in source_tags:
                source_tag = source_tags[inp]

            inp.init_stream_source(ctx=ctx, source_tag=source_tag)

        # TODO we should recursively reconstruct whole tree
        terminal_node = pipeline.func(target.__class__, *pipeline.inputs)
        stream: DataStream = terminal_node.stream
        # TODO configure sink
        s = stream.sink(print)
        # s = stream.sink(lambda e: None)

        # jgb = JobGraphBuilder(stream_sinks=[s])
        # jg = jgb.build()
        # print(jg.gen_digraph())

        ray.init(address='auto')
        ctx.execute()
        time.sleep(1)
        ray.shutdown()

