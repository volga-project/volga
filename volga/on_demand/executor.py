from typing import List, Dict, Any, Set, Optional, Type
from collections import defaultdict
import asyncio
import logging
import time

from volga.api.feature import Feature
from volga.api.pipeline import PipelineFeature
from volga.api.on_demand import OnDemandFeature
from volga.stats.stats_manager import HistogramStats
from volga.streaming.common.utils import now_ts_ms
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.on_demand.models import OnDemandRequest

logger = logging.getLogger(__name__)


class OnDemandExecutor:
    # Special delimiter that's unlikely to be in user-defined names
    PIPELINE_NODE_DELIMITER = "::pipeline_for::"

    def __init__(self, data_connector: OnDemandDataConnector, db_stats: Optional[HistogramStats] = None):
        self._data_connector = data_connector
        self._features = None
        
        self._pipeline_features: Dict[str, PipelineFeature] = {}
        self._ondemand_features: Dict[str, OnDemandFeature] = {}
        
        # Map pipeline features to their dependent on-demand features and query types
        self._pipeline_to_on_demand_query: Dict[str, Dict[str, str]] = {}
        
        # Build dependency graph for execution order
        self._dependency_graph: Dict[str, Set[str]] = {}

        self._db_stats = db_stats
    
    @classmethod
    def get_pipeline_node_name(cls, pipeline_name: str, dependent_name: str) -> str:
        """Generate unique node name for pipeline feature based on dependent feature"""
        return f"{pipeline_name}{cls.PIPELINE_NODE_DELIMITER}{dependent_name}"

    @classmethod
    def parse_pipeline_node_name(cls, node_name: str) -> tuple[str, str]:
        """Extract pipeline and dependent feature names from pipeline node name"""
        if cls.PIPELINE_NODE_DELIMITER not in node_name:
            raise ValueError(f"Not a pipeline node name: {node_name}")
        pipeline_name, dependent_name = node_name.split(cls.PIPELINE_NODE_DELIMITER)
        return pipeline_name, dependent_name

    def _is_pipeline_node(self, name: str) -> bool:
        """Check if a name represents a pipeline node"""
        return self.PIPELINE_NODE_DELIMITER in name
    
    def register_features(self, features: Dict[str, Feature]):  
        self._features = features
        self._init_features()

    def _init_features(self):
        assert self._features is not None
        """Initialize feature categorization and dependencies"""
        for name, feature in self._features.items():
            if isinstance(feature, PipelineFeature):
                self._pipeline_features[name] = feature
                self._pipeline_to_on_demand_query[name] = {}
                self._dependency_graph[name] = set()
            elif isinstance(feature, OnDemandFeature):
                self._ondemand_features[name] = feature
                self._dependency_graph[name] = set()
                
                # Add pipeline dependencies with unique nodes
                for dep_arg in feature.dep_args:
                    dep_name = dep_arg.get_name()
                    if dep_name in self._pipeline_features:
                        # Create unique pipeline node for this dependency
                        pipeline_node = self.get_pipeline_node_name(dep_name, name)
                        self._dependency_graph[pipeline_node] = set()
                        self._dependency_graph[name].add(pipeline_node)
                        # Store query type
                        self._pipeline_to_on_demand_query[dep_name][name] = dep_arg.query_name or 'latest'
                    else:
                        # Regular dependency
                        self._dependency_graph[name].add(dep_name)

    def _get_execution_order(self, target_features: List[str]) -> List[List[str]]:
        """Returns list of feature lists for parallel execution"""
        # Find all required features including dependencies
        required_features = set()
        to_process = set(target_features)
        while to_process:
            feature = to_process.pop()
            required_features.add(feature)
            deps = self._dependency_graph.get(feature, set())
            to_process.update(deps - required_features)

        # Group features by execution level
        feature_levels: Dict[str, int] = {}
        current_level = 0
        
        while len(feature_levels) < len(required_features):
            # Find features whose dependencies are all satisfied
            current_features = {
                f for f in required_features 
                if f not in feature_levels and
                all(dep in feature_levels for dep in self._dependency_graph.get(f, set()))
            }
            
            if not current_features:
                remaining = required_features - set(feature_levels.keys())
                raise ValueError(f"Unable to resolve execution order for features: {remaining}")
            
            for f in current_features:
                feature_levels[f] = current_level
            current_level += 1

        # Group features by level
        levels: Dict[int, List[str]] = defaultdict(list)
        for feature, level in feature_levels.items():
            levels[level].append(feature)

        # Return features grouped by level
        return [levels[i] for i in range(current_level)]

    async def _fetch_pipeline_feature(
        self,
        feature_name: str,
        keys: List[Dict[str, Any]],
        output_type: Type,
        query_name: str,
        query_args: Optional[Dict[str, Any]] = None
    ) -> List[List[Any]]:
        """Fetch a pipeline feature value from storage"""
        try:
            # Use default query if none specified
            query_name = query_name or 'latest'
            
            # Get query function and validate arguments
            query_func = self._data_connector.query_dict()[query_name]
            
            start_ts = time.perf_counter()
            results = await query_func(
                feature_name=feature_name,
                keys=keys,
                **(query_args or {})
            )

            if self._db_stats:
                latency_ms = int((time.perf_counter() - start_ts) * 1000)
                self._db_stats.observe(latency_ms, now_ts_ms())       
            
            # Validate result type
            if not isinstance(results, list):
                raise TypeError(f"Query result must be a list, got {type(results)}")
            
            for sublist in results:
                if not isinstance(sublist, list):
                    raise TypeError(f"Query result must be a list of lists, got list of {type(sublist)}")
                for item in sublist:
                    if not isinstance(item, output_type):
                        raise TypeError(
                            f"Items in query result must be instances of {output_type.__name__}, "
                            f"got {type(item).__name__}"
                        )
            
            return results
            
        except Exception as e:
            raise ValueError(
                f"Error fetching pipeline feature {feature_name}: {str(e)}"
            ) from e

    async def _execute_feature(
        self,
        feature_name: str,
        request: OnDemandRequest,
        computed_values: Dict[str, List[List[Any]]],
        key_idx: int
    ) -> List[Any]:
        """Execute a single feature"""
        feature = self._ondemand_features[feature_name]

        # Get type hints from feature function
        type_hints = list(feature.func.__annotations__.values())[:-1]  # Exclude return type
        
        # Get dependencies
        args = []

        for dep_arg, arg_type in zip(feature.dep_args, type_hints):
            dep_name = dep_arg.get_name()
            if dep_name in self._pipeline_features:
                # Get value from unique pipeline node
                pipeline_node = self.get_pipeline_node_name(dep_name, feature_name)
                dep_values = computed_values[pipeline_node][key_idx]
            else:
                dep_values = computed_values[dep_name][key_idx]

            # If type hint is List, pass the list directly
            if getattr(arg_type, "__origin__", None) is list:
                args.append(dep_values)
            # Otherwise, take first element for non-list arguments
            else:
                args.append(dep_values[0])
             
        result = feature.execute(
            args,
            request.udf_args.get(feature_name)
        )
        
        if not isinstance(result, list):
            result = [result]
        return result


    # TODO test in validate_request
    def _get_num_keys(
        self,
        feature_name: str,
        request: OnDemandRequest,
        visited: Dict[str, int] = None
    ) -> int:
        """
        Recursively determine number of keys for a feature.
        Returns number of keys or raises ValueError if inconsistent.
        """
        if visited is None:
            visited = {}
            
        if feature_name in visited:
            return visited[feature_name]
            
        # If feature has explicit keys, use that
        if feature_name in request.feature_keys:
            visited[feature_name] = len(request.feature_keys[feature_name])
            return visited[feature_name]
            
        feature = self._features[feature_name]
        
        # For pipeline features without explicit keys, raise error
        if not isinstance(feature, OnDemandFeature):
            raise ValueError(f"Pipeline feature {feature_name} requires keys in request")
            
        # Get all dependency paths and their key counts
        dep_keys = {}
        for dep_arg in feature.dep_args:
            dep_name = dep_arg.get_name()
            dep_feature = self._features[dep_name]
            
            # For pipeline features, check if they have keys in request
            if isinstance(dep_feature, PipelineFeature):
                if dep_name not in request.feature_keys:
                    raise ValueError(f"Pipeline feature {dep_name} requires keys in request")
                dep_keys[dep_name] = len(request.feature_keys[dep_name])
            # For on-demand features, recurse
            else:
                num_keys = self._get_num_keys(dep_name, request, visited)
                dep_keys[dep_name] = num_keys

        # Check all dependencies have same number of keys
        if not dep_keys:
            raise ValueError(f"Feature {feature_name} has no dependencies with keys")
            
        key_counts = set(dep_keys.values())
        if len(key_counts) > 1:
            raise ValueError(
                f"Inconsistent number of keys in dependencies for feature {feature_name}. "
                f"Dependencies: {dep_keys}"
            )
            
        visited[feature_name] = key_counts.pop()
        return visited[feature_name]

    async def execute(self, request: OnDemandRequest) -> Dict[str, List[List[Any]]]:
        assert self._features is not None
        request.validate_request(
            features=self._features,
            query_params=self._data_connector.query_params()
        )

        # Store computed values in local variable
        computed_values: Dict[str, List[List[Any]]] = {}

        # Get execution order
        execution_order = self._get_execution_order(request.target_features)

        # Execute features in order
        for level in execution_order:
            # Execute features in current level concurrently
            tasks = []
            for feature_name in level:
                if self._is_pipeline_node(feature_name):
                    # This is a pipeline node
                    pipeline_name, dependent_name = self.parse_pipeline_node_name(feature_name)
                    task = self._fetch_pipeline_feature(
                        pipeline_name,
                        request.feature_keys[dependent_name],
                        self._features[pipeline_name].output_type,
                        self._pipeline_to_on_demand_query[pipeline_name][dependent_name],
                        (request.query_args or {}).get(dependent_name, {})
                    )
                    tasks.append((feature_name, asyncio.create_task(task)))
                else:
                    # Regular feature execution
                    feature_tasks = []
                    num_keys = self._get_num_keys(feature_name, request, {})
                    for key_idx in range(num_keys):
                        task = self._execute_feature(
                            feature_name,
                            request,
                            computed_values,
                            key_idx
                        )
                        feature_tasks.append(asyncio.create_task(task))
                    tasks.append((feature_name, feature_tasks))

            # Wait for all tasks in this level
            for feature_name, task_or_tasks in tasks:
                try:
                    if isinstance(task_or_tasks, list):
                        results = await asyncio.gather(*task_or_tasks)
                        computed_values[feature_name] = results
                    else:
                        computed_values[feature_name] = await task_or_tasks
                except Exception as e:
                    raise ValueError(f"Error executing feature {feature_name}: {str(e)}") from e

        # Return only requested features
        return {
            name: computed_values[name]
            for name in request.target_features
        }

