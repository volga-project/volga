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
    sink_functions: Optional[Dict[str, FunctionOrCallable]] = None,
    params: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, StreamSink]:
    """
    Build a stream graph by traversing the provided features and their dependencies.
    
    Args:
        feature_names: List of feature names to include in the stream graph
        ctx: StreamingContext to use for creating streams
        sink_functions: Optional dictionary mapping feature names to sink functions
        params: Optional nested dictionary mapping feature names to parameter dictionaries
        
    Returns:
        Dictionary mapping feature names to their corresponding StreamSink objects
    """
    # Initialize params if None
    if params is None:
        params = {}
        
    # Get all dependent features at once
    feature_lookup = FeatureRepository.get_dependent_features(feature_names)
    
    # Build dependency graph
    dependency_graph: Dict[str, List[str]] = {}
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
    
    # Track all initialized nodes across all features
    all_initialized_nodes: Set[OperatorNodeBase] = set()
    
    # Process features in topological order to ensure dependencies are initialized first
    processed_features = set()
    
    def process_feature(feature_name: str):
        if feature_name in processed_features:
            return
            
        # Process dependencies first
        for dep_name in dependency_graph[feature_name]:
            process_feature(dep_name)
            
        # Initialize this feature
        entity = initialize_stream(
            feature_name=feature_name,
            initialized=initialized_entities,
            dependency_graph=dependency_graph,
            source_features=source_features,
            ctx=ctx,
            feature_lookup=feature_lookup,
            all_initialized_nodes=all_initialized_nodes,
            params=params
        )
        
        initialized_entities[feature_name] = entity
        processed_features.add(feature_name)
    
    # Process all features
    for feature_name in feature_names:
        process_feature(feature_name)

    # Create dictionary mapping feature names to StreamSink objects
    sink_dict: Dict[str, StreamSink] = {}
    for feature_name in feature_names:
        entity = initialized_entities[feature_name]
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
    dependency_graph: Dict[str, List[str]],
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
        dependency_graph[feature_name] = []
    else:
        dependencies = []
        for dep_arg in feature.dep_args:
            dep_name = dep_arg.get_name()
            if dep_name not in dependencies:
                dependencies.append(dep_name)
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
    dependency_graph: Dict[str, List[str]],
    source_features: Set[str],
    ctx: StreamingContext,
    feature_lookup: Dict[str, PipelineFeature],
    all_initialized_nodes: Set[OperatorNodeBase],
    params: Dict[str, Dict[str, Any]]
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
    
    # Get dependencies in the order they're defined in the pipeline
    ordered_deps = [dep_arg.get_name() for dep_arg in feature.dep_args]
    
    # Assert that ordered_deps contains the same values as dependency_graph[feature_name]
    assert set(ordered_deps) == set(dependency_graph[feature_name]), \
        f"Ordered dependencies {ordered_deps} don't match dependency graph {dependency_graph[feature_name]} for feature {feature_name}"
    
    for dep_name in ordered_deps:
        dep_entity = initialize_stream(
            feature_name=dep_name,
            initialized=initialized,
            dependency_graph=dependency_graph,
            source_features=source_features,
            ctx=ctx,
            feature_lookup=feature_lookup,
            all_initialized_nodes=all_initialized_nodes,
            params=params
        )
        dep_entities.append(dep_entity)
    
    # Initialize the entity and its stream
    is_source = feature_name in source_features
    if is_source and len(dep_entities) > 0:
        raise ValueError(f"Source feature {feature_name} should not have dependencies, but has: {dep_entities}")
    
    # Get feature parameters if any
    feature_params = {}
    if hasattr(feature, 'param_names') and feature.param_names:
        # Get feature-specific parameters
        feature_specific_params = params.get(feature_name, {})
        
        # Get global parameters (used as fallback)
        global_params = params.get('global', {})
        
        for param_name in feature.param_names:
            if param_name in feature_specific_params:
                feature_params[param_name] = feature_specific_params[param_name]
            elif param_name in global_params:
                feature_params[param_name] = global_params[param_name]
            elif hasattr(feature, 'param_defaults') and param_name in feature.param_defaults:
                feature_params[param_name] = feature.param_defaults[param_name]
            else:
                raise ValueError(f"Parameter '{param_name}' required for feature '{feature_name}' but not provided")
    
    entity = initialize_entity_stream(
        feature=feature,
        ctx=ctx,
        is_source=is_source,
        dep_entities=dep_entities,
        all_initialized_nodes=all_initialized_nodes,
        params=feature_params
    )
    
    # Store the entity
    initialized[feature_name] = entity
    return entity


def initialize_entity_stream(
    feature: PipelineFeature,
    ctx: StreamingContext,
    is_source: bool,
    dep_entities: List[Entity],
    all_initialized_nodes: Set[OperatorNodeBase],
    params: Dict[str, Any]
) -> Entity:
    """
    Create and initialize an entity and its stream based on the feature type.
    
    Args:
        feature: The feature to create an entity for
        ctx: The streaming context
        is_source: Whether this is a source feature
        dep_entities: List of dependency entities for non-source features
        all_initialized_nodes: Set of all initialized nodes across all features
        params: Dictionary of parameter values for this feature
        
    Returns:
        The initialized entity
    """
    if is_source:
        # Create source operator node
        source_connector = feature.func(**params) if params else feature.func()
        source_operator = SourceNode(
            source_connector=source_connector,
            ctx=ctx,
            entity_type=feature.output_type
        )
        # Initialize stream and return as entity
        init_operator_chain(
            source_operator, 
            all_initialized_nodes, 
            feature.name
        )
        return source_operator.as_entity(feature.output_type)
        
    else:
        # For non-source features, execute pipeline function with dependency entities and params
        if dep_entities is None:
            dep_entities = []
            
        # Call the function with dependencies and parameters
        if params:
            result_node: OperatorNodeBase = feature.func(*dep_entities, **params)
        else:
            result_node: OperatorNodeBase = feature.func(*dep_entities)
            
        # Initialize all operator nodes in the chain bottom-up
        init_operator_chain(
            result_node, 
            all_initialized_nodes, 
            feature.name
        )
        # Cast to entity type
        return result_node.as_entity(feature.output_type)


def init_operator_chain(
    node: OperatorNodeBase, 
    all_initialized_nodes: Set[OperatorNodeBase],
    feature_name: str
) -> None:
    """
    Recursively initialize all operator nodes in the chain, bottom-up.
    Ensures each node is initialized exactly once across all features.
    
    Args:
        node: The operator node to initialize
        all_initialized_nodes: Set of all initialized nodes across all features
        feature_name: Name of the feature this operator chain belongs to
    """
    def init_node(node: OperatorNodeBase) -> None:
        # Skip if already initialized
        if node in all_initialized_nodes:
            return
            
        # First initialize all parent nodes
        for parent in node.parents:
            init_node(parent)
            
        # Then initialize this node
        node.init_stream()
        
        # Set the stream name
        node.set_stream_name(feature_name)
        
        # Mark as initialized
        all_initialized_nodes.add(node)

    init_node(node)