from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List

import pandas as pd

from volga.common.time_utils import datetime_to_ts
from volga.api.entity import Entity
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.api.storage import InMemoryActorPipelineDataConnector, PipelineDataConnector
from volga.api.pipeline import PipelineFeature
from volga.api.stream_builder import build_stream_graph

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

ScalingConfig = Dict # TODO move


class Client:

    def __init__(self):
        pass

    def materialize(
        self,
        features: List[PipelineFeature],
        parallelism: int = 1,
        pipeline_data_connector: PipelineDataConnector = InMemoryActorPipelineDataConnector(batch=False),
        scaling_config: Optional[ScalingConfig] = None,
        _async: bool = False
    ):
        if scaling_config is not None:
            raise ValueError('ScalingConfig is not supported yet')
        ctx = StreamingContext(job_config=DEFAULT_STREAMING_JOB_CONFIG)
        sink_functions = {
            feature.name: pipeline_data_connector.get_sink_function(feature.name, feature.output_type._entity_metadata.schema())
            for feature in features
        }
        build_stream_graph(features, ctx, sink_functions)
        if _async:
            ctx.submit()
        else:
            ctx.execute()


    # def materialize_online(
    #     self,
    #     target: Entity,
    #     source_tags: Optional[Dict[Entity, str]] = None,
    #     parallelism: int = 1,
    #     scaling_config: Optional[ScalingConfig] = None,
    #     _async: bool = False
    # ):
    #     if scaling_config is not None:
    #         raise ValueError('ScalingConfig is not supported yet')
    #     stream, ctx = self._build_stream(target=target, source_tags=source_tags)
    #     if self.hot is None:
    #         raise ValueError('Online materialization requires HotStorage')
    #     if not isinstance(self.hot, SimpleInMemoryActorStorage):
    #         raise ValueError('Currently only SimpleInMemoryActorStorage is supported')
    #     stream.sink(
    #         self.hot.gen_sink_function(dataset_name=target.__name__, output_schema=target.schema(), hot=True)
    #     )
    #     # stream.sink(print)
    #     if _async:
    #         ctx.submit()
    #     else:
    #         ctx.execute()

    def get_offline_data(
        self,
        dataset_name: str,
        keys: Optional[List[Dict[str, Any]]],
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
        keys: Optional[List[Dict[str, Any]]]
    ) -> Any:
        if self.hot is None:
            raise ValueError('HotStorage is not set')
        return self.hot.get_latest_data(dataset_name=dataset_name, keys=keys)

    def get_on_demand(
        self,
        target: Entity,
        online: bool, # False for offline storage source
        start: Optional[datetime], end: Optional[datetime], # datetime range in case of offline request
        inputs: List[Dict]
    ) -> Any:
        raise NotImplementedError()

    # def _build_stream(self, target: Entity, source_tags: Optional[Dict[Entity, str]]) -> Tuple[DataStream, StreamingContext]:
    #     ctx = StreamingContext(job_config=DEFAULT_STREAMING_JOB_CONFIG)
    #     pipeline = target.get_pipeline()

    #     # build operator graph
    #     # TODO we should recursively reconstruct whole graph in case inputs are not terminal
    #     terminal_operator_node = pipeline.func(target.__class__, *pipeline.inputs)

    #     # build stream graph
    #     self._init_stream_graph(terminal_operator_node, ctx, target.schema(), source_tags)
    #     stream: DataStream = terminal_operator_node.stream

    #     return stream, ctx

    # def _init_stream_graph(
    #     self,
    #     operator_node: OperatorNodeBase,
    #     ctx: StreamingContext,
    #     target_schema: Schema,
    #     source_tags: Optional[Dict[Entity, str]] = None
    # ):

    #     for p in operator_node.parents:
    #         self._init_stream_graph(
    #             p, ctx, target_schema, source_tags
    #         )

    #     # init sources
    #     if isinstance(operator_node, Entity):
    #         if not operator_node.is_source():
    #             raise ValueError('Currently only source inputs are allowed')
    #         source_tag = None
    #         if source_tags is not None and operator_node in source_tags:
    #             source_tag = source_tags[operator_node]

    #         operator_node.init_stream(ctx=ctx, source_tag=source_tag)
    #         return

    #     # TODO special cases i.e. terminal node, join, aggregate, etc.
    #     if isinstance(operator_node, Aggregate):
    #         operator_node.init_stream(target_schema)
    #     else:
    #         operator_node.init_stream()

