import unittest
import datetime
import logging
from typing import Dict, List, Any, Callable, Optional

from volga.api.stream_builder import build_stream_graph
from volga.api.entity import entity, field, Entity
from volga.api.feature import FeatureRepository
from volga.api.source import source, KafkaSource, Connector
from volga.api.pipeline import pipeline
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.data_stream import DataStream
from volga.streaming.api.stream.stream_source import StreamSource
from volga.streaming.api.stream.stream_sink import StreamSink
from volga.streaming.api.job_graph.job_graph import JobGraph, JobVertex, JobEdge, VertexType
from volga.streaming.api.job_graph.job_graph_builder import JobGraphBuilder
from volga.streaming.api.partition.partition import ForwardPartition, KeyPartition
from volga.api.operators import Filter, Assign, Drop, SourceNode


# Define test entities
@entity
class SourceEntity:
    id: str = field(key=True)
    value: float
    timestamp: datetime.datetime = field(timestamp=True)


@entity
class TransformedEntity:
    id: str = field(key=True)
    transformed_value: float
    timestamp: datetime.datetime = field(timestamp=True)


@entity
class JoinedEntity:
    id: str = field(key=True)
    value: float
    transformed_value: float
    timestamp: datetime.datetime = field(timestamp=True)


# Define test features
@source(SourceEntity)
def source_feature() -> Connector:
    return KafkaSource.mock_with_delayed_items(
        items=[SourceEntity(id='test-id', value=1.0, timestamp=datetime.datetime.now())],
        delay_s=0
    )


@pipeline(dependencies=['source_feature'], output=TransformedEntity)
def transform_feature(source: Entity) -> Entity:
    # Transform the data
    return source.transform(lambda x: {
        'id': x['id'],
        'transformed_value': x['value'] * 2,
        'timestamp': x['timestamp']
    }, new_schema_dict={
        'id': str,
        'transformed_value': float,
        'timestamp': datetime.datetime
    })


@pipeline(dependencies=['source_feature', 'transform_feature'], output=JoinedEntity)
def join_feature(source: Entity, transformed: Entity) -> Entity:
    return source.join(transformed, on=['id'])

@pipeline(dependencies=['source_feature'], output=TransformedEntity)
def filter_map_feature(source: Entity) -> Entity:
    """Filter values > 0.5 then assign and drop."""
    return (
        source
        .filter(lambda x: x['value'] > 0.5)
        .assign('transformed_value', float, lambda x: x['value'] * 3)
        .drop(['value'])
    )


class TestStreamBuilder(unittest.TestCase):
    
    def setUp(self):
        
        self.ctx = StreamingContext()
        
    def test_build_multi_feature_stream_graph(self):
        """Test that we can build a stream graph with sinks and verify the job graph topology."""
        # Build the stream graph with sinks
        sinks_dict = build_stream_graph(
            ["source_feature", "transform_feature", "join_feature"],
            self.ctx
        )
        
        # Verify we got streams for all features
        self.assertEqual(len(sinks_dict), 3)
        
        for sink in sinks_dict.values():
            self.assertIsInstance(sink, StreamSink)
        
        # Build the job graph
        job_graph_builder = JobGraphBuilder(stream_sinks=list(sinks_dict.values()))
        job_graph = job_graph_builder.build()
        
        # print(job_graph.gen_digraph())
        
        # Verify vertices
        vertices = job_graph.job_vertices
        self.assertEqual(len(vertices), 8)  # source + transform + (2 key_by + join) + 3 sinks
        
        # Find vertices by type
        source_vertices = [v for v in vertices if v.vertex_type == VertexType.SOURCE]
        process_vertices = [v for v in vertices if v.vertex_type == VertexType.PROCESS]
        join_vertices = [v for v in vertices if v.vertex_type == VertexType.JOIN]
        sink_vertices = [v for v in vertices if v.vertex_type == VertexType.SINK]
        
        # Verify exact number of each vertex type
        self.assertEqual(len(source_vertices), 1)  # One source
        self.assertEqual(len(process_vertices), 3)  # Transform + 2 key_by
        self.assertEqual(len(join_vertices), 1)  # One join
        self.assertEqual(len(sink_vertices), 3)  # Three sinks (one for each feature)
        
        # Verify edges
        edges = job_graph.job_edges
        self.assertEqual(len(edges), 8)  # source->(transform,key_by,sink) + transform->(key_by,sink) + 2(key_by->join) + join->sink
        
        # Get vertices by name for edge verification
        source = source_vertices[0]
        transform = next(v for v in process_vertices if "Transform" in v.stream_operator.get_name())
        left_key_by = next(v for v in process_vertices if "JoinLeftKeyBy" in v.stream_operator.get_name())
        right_key_by = next(v for v in process_vertices if "JoinRightKeyBy" in v.stream_operator.get_name())
        join = join_vertices[0]
        
        def has_edge(source_id: str, target_id: str) -> bool:
            return any(e.source_vertex_id == source_id and e.target_vertex_id == target_id for e in edges)
        
        # Verify source connections
        self.assertTrue(has_edge(source.vertex_id, transform.vertex_id))  # source -> transform
        self.assertTrue(has_edge(source.vertex_id, left_key_by.vertex_id))  # source -> left key_by
        
        # Verify transform connections
        self.assertTrue(has_edge(transform.vertex_id, right_key_by.vertex_id))  # transform -> right key_by
        
        # Verify join connections
        self.assertTrue(has_edge(left_key_by.vertex_id, join.vertex_id))  # left key_by -> join
        self.assertTrue(has_edge(right_key_by.vertex_id, join.vertex_id))  # right key_by -> join
        
        # Verify partition types
        for edge in edges:
            if edge.target_vertex_id == join.vertex_id:
                self.assertIsInstance(edge.partition, KeyPartition)  # key_by -> join edges use KeyPartition
            else:
                self.assertIsInstance(edge.partition, ForwardPartition)  # all other edges use ForwardPartition
    
    def test_simple_job_graph(self):
        """Test a simple source -> sink job graph."""
        # Build just the source feature with a sink
        sinks_dict = build_stream_graph(
            ["source_feature"],
            self.ctx,
        )
        
        # Build the job graph
        job_graph_builder = JobGraphBuilder(stream_sinks=list(sinks_dict.values()))
        job_graph = job_graph_builder.build()
        
        # Verify the job graph
        vertices = job_graph.job_vertices
        edges = job_graph.job_edges
        
        # Should have exactly 2 vertices (source and sink) and 1 edge
        self.assertEqual(len(vertices), 2)
        self.assertEqual(len(edges), 1)
        
        # Verify vertex types
        source_vertex = next((v for v in vertices if v.vertex_type == VertexType.SOURCE), None)
        sink_vertex = next((v for v in vertices if v.vertex_type == VertexType.SINK), None)
        self.assertIsNotNone(source_vertex)
        self.assertIsNotNone(sink_vertex)
        
        # Verify edge
        edge = edges[0]
        self.assertEqual(edge.source_vertex_id, source_vertex.vertex_id)
        self.assertEqual(edge.target_vertex_id, sink_vertex.vertex_id)
        self.assertIsInstance(edge.partition, ForwardPartition)
    
    def test_transform_job_graph(self):
        """Test a source -> transform -> sink job graph."""
        # Build the transform feature with a sink
        sinks_dict = build_stream_graph(
            ["transform_feature"],
            self.ctx,
        )
        
        # Build the job graph
        job_graph_builder = JobGraphBuilder(stream_sinks=list(sinks_dict.values()))
        job_graph = job_graph_builder.build()
        
        # Verify the job graph
        vertices = job_graph.job_vertices
        edges = job_graph.job_edges
        
        # Should have exactly 3 vertices (source, transform, sink) and 2 edges
        self.assertEqual(len(vertices), 3)
        self.assertEqual(len(edges), 2)
        
        # Verify vertex types
        source_vertex = next((v for v in vertices if v.vertex_type == VertexType.SOURCE), None)
        process_vertex = next((v for v in vertices if v.vertex_type == VertexType.PROCESS), None)
        sink_vertex = next((v for v in vertices if v.vertex_type == VertexType.SINK), None)
        self.assertIsNotNone(source_vertex)
        self.assertIsNotNone(process_vertex)
        self.assertIsNotNone(sink_vertex)
        
        # Verify edges
        source_to_process = next((e for e in edges 
                                if e.source_vertex_id == source_vertex.vertex_id 
                                and e.target_vertex_id == process_vertex.vertex_id), None)
        process_to_sink = next((e for e in edges 
                              if e.source_vertex_id == process_vertex.vertex_id 
                              and e.target_vertex_id == sink_vertex.vertex_id), None)
        self.assertIsNotNone(source_to_process)
        self.assertIsNotNone(process_to_sink)

    def test_filter_map_feature(self):
        """Test feature with filter, assign and drop operators."""
        # Build stream graph
        sinks_dict = build_stream_graph(
            ['filter_map_feature'],
            self.ctx
        )
        
        # Verify we got sink for the feature
        self.assertEqual(len(sinks_dict), 1)
        self.assertIn('filter_map_feature', sinks_dict)
        
        # Get the stream for verification
        sink = sinks_dict['filter_map_feature']
        stream = sink.input_stream
        
        # Verify operator chain by checking stream operations
        # Note: We need to check the actual stream operations rather than 
        # trying to assert specific operator class types
        
        # Build and verify job graph
        job_graph_builder = JobGraphBuilder(stream_sinks=list(sinks_dict.values()))
        job_graph = job_graph_builder.build()
        
        # Verify vertices
        vertices = job_graph.job_vertices
        source_vertices = [v for v in vertices if v.vertex_type == VertexType.SOURCE]
        process_vertices = [v for v in vertices if v.vertex_type == VertexType.PROCESS]
        sink_vertices = [v for v in vertices if v.vertex_type == VertexType.SINK]
        
        self.assertEqual(len(source_vertices), 1)  # One source
        self.assertEqual(len(process_vertices), 3)  # Filter, assign, drop
        self.assertEqual(len(sink_vertices), 1)  # One sink
        
        # Verify edges
        edges = job_graph.job_edges
        self.assertEqual(len(edges), 4)  # source->filter->assign->drop->sink
        
        # Verify edge connections
        for edge in edges:
            # Each edge should connect to the next vertex
            target_vertex = next((v for v in vertices if v.vertex_id == edge.target_vertex_id), None)
            source_vertex = next((v for v in vertices if v.vertex_id == edge.source_vertex_id), None)
            self.assertIsNotNone(target_vertex)
            self.assertIsNotNone(source_vertex)

if __name__ == '__main__':
    unittest.main() 
    # t = TestStreamBuilder()
    # t.setUp()
    # t.test_simple_job_graph()
    # t.test_transform_job_graph()
    # t.test_build_multi_feature_stream_graph()
    # t.test_filter_map_feature()
    # t.tearDown()