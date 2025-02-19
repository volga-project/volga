from typing import List, Dict, Any, Set, Optional, Type
from collections import defaultdict
import asyncio
import logging
import time

from volga.api.feature import FeatureRepository
from volga.api.pipeline import PipelineFeature
from volga.api.on_demand import OnDemandFeature
from volga.stats.stats_manager import HistogramStats
from volga.streaming.common.utils import now_ts_ms
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from volga.on_demand.on_demand import OnDemandRequest

logger = logging.getLogger(__name__)


class OnDemandExecutor:
    # Special delimiter that's unlikely to be in user-defined names
    PIPELINE_NODE_DELIMITER = "::pipeline_for::"

    def __init__(self, data_connector: OnDemandDataConnector):
        self._data_connector = data_connector
        self._features = FeatureRepository.get_all_features()
        
        # Categorize features
        self._pipeline_features: Dict[str, PipelineFeature] = {}
        self._ondemand_features: Dict[str, OnDemandFeature] = {}
        
        # Map pipeline features to their dependent on-demand features and query types
        self._pipeline_to_on_demand_query: Dict[str, Dict[str, str]] = {}
        
        # Build dependency graph for execution order
        self._dependency_graph: Dict[str, Set[str]] = {}
        
        self._init_features()
    
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

    def _init_features(self):
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
        keys: Dict[str, Any],
        output_type: Type,
        query_name: str,
        query_args: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Fetch a pipeline feature value from storage"""
        try:
            # Use default query if none specified
            query_name = query_name or 'latest'
            
            # Get query function and validate arguments
            query_func = self._data_connector.query_dict()[query_name]
            
            # Execute query
            result = await query_func(
                feature_name=feature_name,
                keys=keys,
                **(query_args or {})
            )
            
            # Validate result type
            if isinstance(result, list):
                for item in result:
                    if not isinstance(item, output_type):
                        raise TypeError(
                            f"Items in query result must be instances of {output_type.__name__}, "
                            f"got {type(item).__name__}"
                        )
            else:
                if not isinstance(result, output_type):
                    raise TypeError(
                        f"Query result must be an instance of {output_type.__name__}, "
                        f"got {type(result).__name__}"
                    )
            
            return result
            
        except Exception as e:
            raise ValueError(
                f"Error fetching pipeline feature {feature_name}: {str(e)}"
            ) from e

    async def _execute_feature(
        self,
        feature_name: str,
        request: OnDemandRequest,
        computed_values: Dict[str, Any]
    ) -> Any:
        """Execute a single feature"""
        feature = self._ondemand_features[feature_name]
        
        # Get dependencies
        dep_values = []
        for dep_arg in feature.dep_args:
            dep_name = dep_arg.get_name()
            if dep_name in self._pipeline_features:
                # Get value from unique pipeline node
                pipeline_node = self.get_pipeline_node_name(dep_name, feature_name)
                dep_value = computed_values[pipeline_node]
            else:
                dep_value = computed_values[dep_name]
            dep_values.append(dep_value)
        
        # Execute feature
        return feature.execute(
            dep_values,
            request.udf_args.get(feature_name)
        )

    async def execute(self, request: OnDemandRequest) -> Dict[str, Any]:
        """Execute on-demand features"""
        request.validate_request(
            features=self._features,
            query_params=self._data_connector.query_params()
        )

        # Store computed values in local variable
        computed_values: Dict[str, Any] = {}

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
                        request.feature_keys.get(dependent_name, {}),
                        self._features[pipeline_name].output_type,
                        self._pipeline_to_on_demand_query[pipeline_name][dependent_name],
                        (request.query_args or {}).get(dependent_name, {})
                    )
                else:
                    # Regular feature execution
                    task = self._execute_feature(
                        feature_name,
                        request,
                        computed_values
                    )
                tasks.append((feature_name, asyncio.create_task(task)))

            # Wait for all tasks in this level
            for feature_name, task in tasks:
                try:
                    computed_values[feature_name] = await task
                except Exception as e:
                    raise ValueError(f"Error executing feature {feature_name}: {str(e)}") from e

        # Return only requested features
        return {
            name: computed_values[name]
            for name in request.target_features
        }

