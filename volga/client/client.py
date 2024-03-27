from datetime import datetime
from typing import Dict, Optional, Tuple, Any

import pandas as pd

from volga.common.time_utils import datetime_to_ts
from volga.data.api.dataset.dataset import Dataset
from volga.data.api.dataset.operator import Aggregate, OperatorNodeBase
from volga.data.api.dataset.schema import DatasetSchema
from volga.storage.cold.cold import ColdStorage
from volga.storage.common.simple_in_memory_actor_storage import SimpleInMemoryActorStorage
from volga.storage.hot.hot import HotStorage
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

    def __init__(self, hot: Optional[HotStorage] = None, cold: Optional[ColdStorage] = None):
        self.cold = cold
        self.hot = hot

    def materialize_offline(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]] = None, _async: bool = False):
        stream, ctx = self._build_stream(target=target, source_tags=source_tags)
        if self.cold is None:
            raise ValueError('Offline materialization requires ColdStorage')
        if not isinstance(self.cold, SimpleInMemoryActorStorage):
            raise ValueError('Currently only SimpleInMemoryActorStorage is supported')
        stream.sink(
            self.cold.gen_sink_function(dataset_name=target.__name__, output_schema=target.dataset_schema(), hot=False)
        )
        # stream.sink(print)
        if _async:
            ctx.submit()
        else:
            ctx.execute()

    def materialize_online(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]] = None, _async: bool = False):
        stream, ctx = self._build_stream(target=target, source_tags=source_tags)
        if self.hot is None:
            raise ValueError('Online materialization requires HotStorage')
        if not isinstance(self.hot, SimpleInMemoryActorStorage):
            raise ValueError('Currently only SimpleInMemoryActorStorage is supported')
        stream.sink(
            self.hot.gen_sink_function(dataset_name=target.__name__, output_schema=target.dataset_schema(), hot=True)
        )
        # stream.sink(print)
        if _async:
            ctx.submit()
        else:
            ctx.execute()

    def get_offline_data(
        self,
        dataset_name: str,
        keys: Optional[Dict[str, Any]],
        start: Optional[datetime],
        end: Optional[datetime]
    ) -> pd.DataFrame:
        if self.cold is None:
            raise ValueError('ColdStorage is not set')
        start_ts = None if start is None else datetime_to_ts(start)
        end_ts = None if end is None else datetime_to_ts(end)
        data = self.cold.get_data(dataset_name=dataset_name, keys=keys, start_ts=start_ts, end_ts=end_ts)
        return pd.DataFrame(data)

    def get_online_latest_data(
        self,
        dataset_name: str,
        keys: Optional[Dict[str, Any]]
    ) -> Any:
        if self.hot is None:
            raise ValueError('HotStorage is not set')
        return self.hot.get_latest_data(dataset_name=dataset_name, keys=keys)

    def _build_stream(self, target: Dataset, source_tags: Optional[Dict[Dataset, str]]) -> Tuple[DataStream, StreamingContext]:
        ctx = StreamingContext(job_config=DEFAULT_STREAMING_JOB_CONFIG)
        pipeline = target.get_pipeline()

        # build operator graph
        # TODO we should recursively reconstruct whole tree in case inputs are not terminal
        terminal_operator_node = pipeline.func(target.__class__, *pipeline.inputs)

        # build stream graph
        self._init_stream_graph(terminal_operator_node, ctx, target.dataset_schema(), source_tags)
        stream: DataStream = terminal_operator_node.stream

        return stream, ctx

    def _init_stream_graph(
        self,
        operator_node: OperatorNodeBase,
        ctx: StreamingContext,
        target_dataset_schema: DatasetSchema,
        source_tags: Optional[Dict[Dataset, str]] = None
    ):

        for p in operator_node.parents:
            self._init_stream_graph(
                p, ctx, target_dataset_schema, source_tags
            )

        # init sources
        if isinstance(operator_node, Dataset):
            if not operator_node.is_source():
                raise ValueError('Currently only source inputs are allowed')
            source_tag = None
            if source_tags is not None and operator_node in source_tags:
                source_tag = source_tags[operator_node]

            operator_node.init_stream(ctx=ctx, source_tag=source_tag)
            return

        # TODO special cases i.e. terminal node, join, aggregate, etc.
        if isinstance(operator_node, Aggregate):
            operator_node.init_stream(target_dataset_schema)
        else:
            operator_node.init_stream()

