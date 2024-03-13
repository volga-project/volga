import time
from typing import Dict, Optional

from ray.util.client import ray

from volga.data.api.dataset.dataset import Dataset
from volga.data.api.dataset.node import Node, Aggregate, NodeBase
from volga.data.api.dataset.schema import DataSetSchema
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

        # build node graph
        # TODO we should recursively reconstruct whole tree in case inputs are not terminal
        terminal_node = pipeline.func(target.__class__, *pipeline.inputs)

        # build stream graph
        self._build_stream_graph(terminal_node, ctx, target.data_set_schema(), source_tags)
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

    def _build_stream_graph(
        self,
        node: NodeBase,
        ctx: StreamingContext,
        target_dataset_schema: DataSetSchema,
        source_tags: Optional[Dict[Dataset, str]] = None
    ):

        for p in node.parents:
            self._build_stream_graph(
                p, ctx, target_dataset_schema, source_tags
            )

        # print(node)
        # init sources
        if isinstance(node, Dataset):
            if not node.is_source():
                raise ValueError('Currently only source inputs are allowed')
            source_tag = None
            if source_tags is not None and node in source_tags:
                source_tag = source_tags[node]

            node.init_stream(ctx=ctx, source_tag=source_tag)
            return

        if isinstance(node, Aggregate):
            node.init_stream(target_dataset_schema)
        else:
            node.init_stream()

