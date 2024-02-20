from typing import List, Dict, Optional

from ray.util.client import ray

from volga.data.api.dataset.dataset import Dataset
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.data_stream import DataStream


class Client:

    def __init__(self):
        pass

    def materialize_offline(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]]):
        ctx = StreamingContext()
        pipeline = target._get_pipeline()

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

        terminal_node = pipeline.func(pipeline.inputs)
        stream: DataStream = terminal_node.stream
        stream.sink(print) # TODO configure sink
        ray.init(adress='auto')
        ctx.execute()
        ray.shutdown()

