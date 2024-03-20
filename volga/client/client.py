import time
from typing import Dict, Optional, Tuple

import ray

from volga.data.api.dataset.dataset import Dataset
from volga.data.api.dataset.node import Node, Aggregate, NodeBase
from volga.data.api.dataset.schema import DataSetSchema
from volga.storage.common.simple_in_memory_actor_storage import SimpleInMemoryActorStorage
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.data_stream import DataStream


DEFAULT_STREAMING_JOB_CONFIG = {
    'worker_config_template': {},
    'master_config': {
        'resource_config': {
            'default_worker_resources': {
                'CPU': '0.01'
            }
        },
        'scheduler_config': {}
    }
}


class Client:

    def __init__(self):
        pass

    def materialize_offline(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]] = None):
        stream, ctx = self._build_stream(target=target, source_tags=source_tags)
        storage = SimpleInMemoryActorStorage(dataset_name=target.__class__.__name__, output_schema=target.data_set_schema())
        stream.sink(storage.gen_sink_function())

        ray.init(address='auto')
        ctx.execute()
        time.sleep(1)

        # storage.get_data()
        ray.shutdown()

    def materialize_online(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]] = None):
        stream, ctx = self._build_stream(target=target, source_tags=source_tags)
        # TODO configure sink with HotStorage
        s = stream.sink(print)

        ray.init(address='auto')
        ctx.execute()
        time.sleep(1)
        ray.shutdown()

    def _build_stream(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]]) -> Tuple[DataStream, StreamingContext]:
        ctx = StreamingContext(job_config=DEFAULT_STREAMING_JOB_CONFIG)
        pipeline = target.get_pipeline()

        # build node graph
        # TODO we should recursively reconstruct whole tree in case inputs are not terminal
        terminal_node = pipeline.func(target.__class__, *pipeline.inputs)

        # build stream graph
        self._init_stream_graph(terminal_node, ctx, target.data_set_schema(), source_tags)
        stream: DataStream = terminal_node.stream

        return stream, ctx

    def _init_stream_graph(
        self,
        node: NodeBase,
        ctx: StreamingContext,
        target_dataset_schema: DataSetSchema,
        source_tags: Optional[Dict[Dataset, str]] = None
    ):

        for p in node.parents:
            self._init_stream_graph(
                p, ctx, target_dataset_schema, source_tags
            )

        # init sources
        if isinstance(node, Dataset):
            if not node.is_source():
                raise ValueError('Currently only source inputs are allowed')
            source_tag = None
            if source_tags is not None and node in source_tags:
                source_tag = source_tags[node]

            node.init_stream(ctx=ctx, source_tag=source_tag)
            return

        # TODO special cases i.e. terminal node, join, aggregate, etc.
        if isinstance(node, Aggregate):
            node.init_stream(target_dataset_schema)
        else:
            node.init_stream()

