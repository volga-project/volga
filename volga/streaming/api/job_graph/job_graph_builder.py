import logging
from typing import List, Dict, Optional

from volga.streaming.api.job_graph.job_graph import JobGraph, JobVertex, VertexType, JobEdge
from volga.streaming.api.stream.data_stream import DataStream, JoinStream, UnionStream
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.api.stream.stream import Stream
from volga.streaming.api.stream.stream_sink import StreamSink

logger = logging.getLogger(__name__)


class JobGraphBuilder:

    def __init__(
        self,
        stream_sinks: List[StreamSink],
        job_name: Optional[str] = None,
        job_config: Optional[Dict] = None,
    ):
        self.job_graph = JobGraph(
            job_name,
            job_config
        )
        self.stream_sinks = stream_sinks

    def build(self) -> JobGraph:
        for stream_sink in self.stream_sinks:
            self._process_stream(stream_sink)
        return self.job_graph

    def _process_stream(self, stream: Stream):
        vertex_id = stream.id
        parallelism = stream.parallelism
        stream_operator = stream.stream_operator
        if isinstance(stream, StreamSink):
            vertex_type = VertexType.SINK
            parent_stream = stream.input_stream
            self.job_graph.add_edge_if_not_exists(JobEdge(
                source_vertex_id=parent_stream.id,
                target_vertex_id=vertex_id,
                partition=parent_stream.partition
            ))
            self._process_stream(parent_stream)
        elif isinstance(stream, StreamSource):
            vertex_type = VertexType.SOURCE
        elif isinstance(stream, DataStream):
            if isinstance(stream, JoinStream):
                vertex_type = VertexType.JOIN
            elif isinstance(stream, UnionStream):
                vertex_type = VertexType.UNION
            else:
                vertex_type = VertexType.PROCESS

            parent_stream = stream.input_stream
            self.job_graph.add_edge_if_not_exists(JobEdge(
                source_vertex_id=parent_stream.id,
                target_vertex_id=vertex_id,
                partition=parent_stream.partition
            ))
            self._process_stream(parent_stream)

            # process union stream
            if isinstance(stream, UnionStream):
                for other_stream in stream.union_streams:
                    self.job_graph.add_edge_if_not_exists(JobEdge(
                        source_vertex_id=other_stream.id,
                        target_vertex_id=vertex_id,
                        partition=other_stream.partition
                    ))
                    self._process_stream(parent_stream)

            # process join stream
            if isinstance(stream, JoinStream):
                right_stream = stream.right_stream
                self.job_graph.add_edge_if_not_exists(JobEdge(
                    source_vertex_id=right_stream.id,
                    target_vertex_id=vertex_id,
                    partition=right_stream.partition
                ))
                self._process_stream(right_stream)

            # TODO MergeStream
        else:
            raise RuntimeError(f'Unsupported stream type: {stream}')

        assert vertex_type is not None
        self.job_graph.add_vertex_if_not_exists(JobVertex(
            vertex_id=vertex_id,
            parallelism=parallelism,
            vertex_type=vertex_type,
            stream_operator=stream_operator
        ))
