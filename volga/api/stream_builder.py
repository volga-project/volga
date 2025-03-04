from typing import Dict, List, Set, Optional, Tuple, Any, Type
from volga.api.entity import Entity, create_entity, EntityMetadata
from volga.api.feature import FeatureRepository
from volga.api.pipeline import PipelineFeature
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream_sink import StreamSink
from volga.streaming.api.message.message import Record
from volga.common.time_utils import datetime_str_to_ts
from decimal import Decimal
from volga.streaming.api.operators.timestamp_assigner import EventTimeAssigner
from volga.api.operators import OperatorNodeBase, SourceNode
from volga.streaming.api.stream.data_stream import FunctionOrCallable
from volga.streaming.api.stream.stream_source import StreamSource


def build_stream_graph(
    feature_names: List[str],
    ctx: StreamingContext,
    sink_functions: Optional[Dict[str, FunctionOrCallable]] = None
) -> Dict[str, StreamSink]:
    """
    Build a stream graph by traversing the provided features and their dependencies.
    
    Args:
        feature_names: List of feature names to include in the stream graph
        ctx: StreamingContext to use for creating streams
        sink_functions: Optional dictionary mapping feature names to sink functions
        
    Returns:
        Dictionary mapping feature names to their corresponding StreamSink objects
    """
    # Get all dependent features at once
    feature_lookup = FeatureRepository.get_dependent_features(feature_names)
    
    # Build dependency graph
    dependency_graph: Dict[str, Set[str]] = {}
    source_features: Set[str] = set()
    visited: Set[str] = set()
    
    for feature_name in feature_lookup.keys():
        build_dependency_graph(
            feature_name, 
            visited, 
            [], 
            dependency_graph, 
            source_features, 
            feature_lookup
        )

    # Initialize streams
    initialized_entities: Dict[str, Entity] = {}
    initialized_nodes: Set[OperatorNodeBase] = set()
    
    # Process all features to ensure the entire graph is built
    for feature_name in feature_lookup.keys():
        initialize_stream(
            feature_name=feature_name,
            initialized=initialized_entities,
            dependency_graph=dependency_graph,
            source_features=source_features,
            ctx=ctx,
            feature_lookup=feature_lookup,
            initialized_nodes=initialized_nodes
        )

    # Create dictionary mapping feature names to StreamSink objects
    sink_dict: Dict[str, StreamSink] = {}
    for feature_name, entity in initialized_entities.items():
        if feature_name in feature_names:
            if sink_functions is None:
                sink_function = print
            else:
                sink_function = sink_functions[feature_name]
            sink = entity.stream.sink(sink_function)
            sink_dict[feature_name] = sink
    
    return sink_dict


def build_dependency_graph(
    feature_name: str,
    visited: Set[str],
    path: List[str],
    dependency_graph: Dict[str, Set[str]],
    source_features: Set[str],
    feature_lookup: Dict[str, PipelineFeature]
) -> None:
    """
    Build a dependency graph for a feature.
    Detects cycles and identifies source features.
    """
    if feature_name in visited:
        if feature_name in path:
            cycle = path[path.index(feature_name):] + [feature_name]
            raise ValueError(f"Cycle detected in feature dependencies: {' -> '.join(cycle)}")
        return
    
    visited.add(feature_name)
    path.append(feature_name)
    
    feature = feature_lookup.get(feature_name)
    if feature is None:
        raise ValueError(f"Feature {feature_name} not found in provided features or repository")
    
    if feature.is_source:
        source_features.add(feature_name)
        dependency_graph[feature_name] = set()
    else:
        dependencies = set()
        for dep_arg in feature.dep_args:
            dep_name = dep_arg.get_name()
            dependencies.add(dep_name)
            build_dependency_graph(
                dep_name, 
                visited, 
                path, 
                dependency_graph, 
                source_features,
                feature_lookup
            )
        dependency_graph[feature_name] = dependencies
    
    path.pop()


def initialize_stream(
    feature_name: str,
    initialized: Dict[str, Entity],
    dependency_graph: Dict[str, Set[str]],
    source_features: Set[str],
    ctx: StreamingContext,
    feature_lookup: Dict[str, PipelineFeature],
    initialized_nodes: Set[OperatorNodeBase]
) -> Entity:
    """
    Recursively initialize streams from source features up.
    Returns the Entity instance for the feature.
    """
    if feature_name in initialized:
        return initialized[feature_name]
        
    feature = feature_lookup.get(feature_name)
    if feature is None:
        raise ValueError(f"Feature {feature_name} not found in provided features or repository")
        
    # Initialize dependencies first, dfs style
    dep_entities = []
    for dep_name in dependency_graph[feature_name]:
        dep_entity = initialize_stream(
            feature_name=dep_name,
            initialized=initialized,
            dependency_graph=dependency_graph,
            source_features=source_features,
            ctx=ctx,
            feature_lookup=feature_lookup,
            initialized_nodes=initialized_nodes
        )
        dep_entities.append(dep_entity)
    
    # Initialize the entity and its stream
    is_source = feature_name in source_features
    entity = initialize_entity_stream(
        feature=feature,
        ctx=ctx,
        is_source=is_source,
        dep_entities=dep_entities,
        initialized_nodes=initialized_nodes
    )
    
    # Store the entity
    initialized[feature_name] = entity
    return entity


def initialize_entity_stream(
    feature: PipelineFeature,
    ctx: StreamingContext,
    is_source: bool,
    dep_entities: List[Entity],
    initialized_nodes: Set[OperatorNodeBase]
) -> Entity:
    """
    Create and initialize an entity and its stream based on the feature type.
    
    Args:
        feature: The feature to create an entity for
        ctx: The streaming context
        is_source: Whether this is a source feature
        dep_entities: List of dependency entities for non-source features
        initialized_nodes: Set of already initialized nodes
        
    Returns:
        The initialized entity
    """
    if is_source:
        # Create source operator node
        source_operator = SourceNode(
            source_connector=feature.func(),
            ctx=ctx,
            entity_type=feature.output_type
        )
        # Initialize stream and return as entity
        init_operator_chain(source_operator, initialized_nodes)
        return source_operator.as_entity(feature.output_type)
        
    else:
        # For non-source features, execute pipeline function with dependency entities
        if dep_entities is None:
            dep_entities = []
            
        result_node: OperatorNodeBase = feature.func(*dep_entities)
        # Initialize all operator nodes in the chain bottom-up
        init_operator_chain(result_node, initialized_nodes)
        # Cast to entity type
        return result_node.as_entity(feature.output_type)


def init_operator_chain(node: OperatorNodeBase, initialized_nodes: Set[OperatorNodeBase]) -> None:
    """
    Recursively initialize all operator nodes in the chain, bottom-up.
    Uses shared initialized_nodes set to ensure each node is initialized only once.
    
    Args:
        node: The operator node to initialize
        initialized_nodes: Set of already initialized nodes
    """
    def init_node(node: OperatorNodeBase) -> None:
        # Skip if already initialized
        if node in initialized_nodes:
            return
            
        # First initialize all parent nodes
        for parent in node.parents:
            init_node(parent)
            
        # Then initialize this node
        node.init_stream()
        initialized_nodes.add(node)

    init_node(node)