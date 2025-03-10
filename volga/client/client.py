from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List

from volga.streaming.api.context.streaming_context import StreamingContext
from volga.api.storage import InMemoryActorPipelineDataConnector, PipelineDataConnector
from volga.api.pipeline import PipelineFeature
from volga.api.stream_builder import build_stream_graph

DEFAULT_STREAMING_JOB_CONFIG = {
    'worker_config': {},
    'master_config': {
        'resource_config': {
            'default_worker_resources': {
                'CPU': '0.01'
            }
        },
        'scheduler_config': {},
        'node_assign_strategy': 'parallelism_first'
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
        job_config: Dict = DEFAULT_STREAMING_JOB_CONFIG,
        _async: bool = False,
        params: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        if scaling_config is not None:
            raise ValueError('ScalingConfig is not supported yet')
        ctx = StreamingContext(job_config=job_config)
        sink_functions = {
            feature.name: pipeline_data_connector.get_sink_function(feature.name, feature.output_type._entity_metadata.schema())
            for feature in features
        }
        build_stream_graph(
            [feature.name for feature in features], 
            ctx, 
            sink_functions,
            params=params
        )
        if _async:
            ctx.submit()
        else:
            ctx.execute()

