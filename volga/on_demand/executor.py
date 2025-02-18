from typing import List, Dict, Any, Set, Optional, Type
from collections import defaultdict
import asyncio
import logging

from volga.api.feature import FeatureRepository
from volga.api.pipeline import PipelineFeature
from volga.on_demand.data.data_service import DataService
from volga.api.on_demand import OnDemandFeature

logger = logging.getLogger(__name__)


class OnDemandExecutor:
    def __init__(self):
        """Initialize executor with all features from FeatureRepository"""
        all_features = FeatureRepository.get_all_features()
        
        # Store on-demand features
        self._ondemand_features = {
            name: feature 
            for name, feature in all_features.items()
            if isinstance(feature, OnDemandFeature)
        }
        
        # Store pipeline features that are dependencies of on-demand features
        self._pipeline_deps = {
            name: feature
            for name, feature in all_features.items()
            if isinstance(feature, PipelineFeature) and
            any(feature in f.dependencies for f in self._ondemand_features.values())
        }
        
        self._dependency_graph = self._build_dependency_graph()
        self._check_circular_dependencies()
        
        logger.debug(f"Built dependency graph: {self._dependency_graph}")

    def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build dependency graph for features"""
        dependency_graph = {}
        for feature_name, feature in self._ondemand_features.items():
            dependency_graph[feature_name] = {
                dep.name for dep in feature.dependencies
            }
        return dependency_graph

    def _check_circular_dependencies(self) -> None:
        """Check for circular dependencies in the graph"""
        visited = set()
        path = set()

        def visit(feature: str) -> None:
            if feature in path:
                path_list = list(path)
                cycle = path_list[path_list.index(feature):] + [feature]
                raise ValueError(f"Circular dependency detected: {' -> '.join(cycle)}")
            
            if feature in visited:
                return
            
            visited.add(feature)
            path.add(feature)
            
            for dep in self._dependency_graph.get(feature, set()):
                visit(dep)
                
            path.remove(feature)

        for feature in self._ondemand_features:
            if feature not in visited:
                visit(feature)

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

        return [levels[i] for i in range(current_level)]

    async def execute(
        self,
        target_features: List[str],
        feature_keys: Dict[str, Dict[str, Any]],
        udf_args: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Execute the computation graph for the target features.
        
        Args:
            target_features: List of feature names to compute
            feature_keys: Dict mapping feature names to their key values
            udf_args: Optional dict mapping feature names to their UDF arguments
        """
        if udf_args is None:
            udf_args = {}

        # Verify all target features are on-demand features
        if not all(f in self._ondemand_features for f in target_features):
            non_ondemand = [
                f for f in target_features 
                if f not in self._ondemand_features and f in FeatureRepository.get_all_features()
            ]
            missing = [
                f for f in target_features 
                if f not in FeatureRepository.get_all_features()
            ]
            
            if non_ondemand:
                raise ValueError(
                    f"Features are not on-demand features: {non_ondemand}"
                )
            if missing:
                raise KeyError(f"Features not found: {missing}")

        logger.debug(f"Executing features: {target_features}")
        execution_order = self._get_execution_order(target_features)
        results: Dict[str, Any] = {}

        # Execute each level in sequence, but features within a level in parallel
        for level_idx, level_features in enumerate(execution_order):
            logger.debug(f"Executing level {level_idx}: {level_features}")
            level_tasks = []
            
            for feature_name in level_features:
                # Check if this is a pipeline feature
                if feature_name in self._pipeline_deps:
                    # Fetch pipeline feature
                    pipeline_feature = self._pipeline_deps[feature_name]
                    task = asyncio.create_task(
                        self._fetch_pipeline_feature(
                            feature_name,
                            feature_keys[feature_name],
                            pipeline_feature.output_type
                        )
                    )
                    level_tasks.append((feature_name, task))
                    continue

                # Handle on-demand feature
                feature = self._ondemand_features[feature_name]
                
                # Get dependency values in order
                dep_values = [
                    results[dep.name]
                    for dep in feature.dependencies
                ]
                
                # Add UDF args if any
                feature_udf_args = udf_args.get(feature_name, {})
                
                # Execute on-demand feature
                task = asyncio.create_task(
                    self._execute_on_demand_feature(
                        feature,
                        dep_values,
                        feature_udf_args
                    )
                )
                level_tasks.append((feature_name, task))

            # Wait for all features in this level to complete
            for feature_name, task in level_tasks:
                try:
                    results[feature_name] = await task
                    logger.debug(f"Completed feature: {feature_name}")
                except Exception as e:
                    if feature_name in self._pipeline_deps:
                        raise ValueError(
                            f"Error fetching pipeline feature {feature_name}: {str(e)}"
                        ) from e
                    else:
                        raise ValueError(
                            f"Error computing feature {feature_name}: {str(e)}"
                        ) from e

        return {f: results[f] for f in target_features}
    
    async def _fetch_pipeline_feature(
        self,
        feature_name: str,
        keys: Dict[str, Any],
        output_type: Type
    ) -> Any:
        """Fetch a pipeline feature value from storage"""
        try:
            return await DataService.fetch_latest(
                feature_name=feature_name,
                keys=keys
            )
        except Exception as e:
            raise ValueError(
                f"Error fetching pipeline feature {feature_name}: {str(e)}"
            ) from e

    async def _execute_on_demand_feature(
        self,
        feature: OnDemandFeature,
        dep_values: List[Any],
        udf_args: Dict[str, Any]
    ) -> Any:
        """Execute an on-demand feature with the given dependencies and arguments"""
        try:
            return feature.execute(dep_values, udf_args)
        except Exception as e:
            raise ValueError(f"Error executing feature: {str(e)}") from e