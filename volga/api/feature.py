from abc import ABC
from typing import Callable, List, Type, Optional, Any, Dict, Union, Tuple
from dataclasses import dataclass

@dataclass
class DepArg:
    """Represents a feature dependency with optional query specification"""
    feature_name: str
    query_name: str = 'latest'

    def get_name(self) -> str:
        """Get the name of the dependent feature"""
        return self.feature_name

def validate_dependencies(feature_name: str, dep_args: List[DepArg]) -> None:
    """Validate dependencies and check for circular dependencies"""
    visited = set()
    
    def check_circular(feat_name: str, path: List[str]) -> None:
        if feat_name in path:
            cycle = path[path.index(feat_name):] + [feat_name]
            raise ValueError(
                f'Circular dependency detected in feature {feature_name}: {" -> ".join(cycle)}'
            )
        
        if feat_name in visited:
            return
            
        visited.add(feat_name)
        path.append(feat_name)
        
        feat = FeatureRepository.get_feature(feat_name)
        if feat is None:
            raise ValueError(
                f'Dependency {feat_name} not found for feature {feature_name}. '
                'Make sure all dependencies are decorated with @source, @pipeline, or @on_demand'
            )
            
        # Get dependency names
        for dep_arg in feat.dep_args:
            check_circular(dep_arg.get_name(), path)
            
        path.pop()
    
    # Validate each dependency
    for dep_arg in dep_args:
        check_circular(dep_arg.get_name(), [])

class Feature(ABC):
    def __init__(
        self,
        func: Callable,
        dep_args: List[DepArg],
        output_type: Type
    ):
        self.func = func
        self.name = func.__name__
        self.output_type = output_type
        self._dep_args = dep_args

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    @property
    def dep_args(self) -> List[DepArg]:
        """Get list of dependency arguments"""
        return self._dep_args

    def get_dependency_names(self) -> List[str]:
        """Get list of dependency names"""
        return [dep_arg.get_name() for dep_arg in self.dep_args]

class FeatureRepository:
    _features: Dict[str, Feature] = {}
    
    @classmethod
    def register(cls, feature: Feature) -> None:
        if feature.name in cls._features:
            raise ValueError(f'Feature {feature.name} already exists')
        cls._features[feature.name] = feature
    
    @classmethod
    def get_feature(cls, name: str) -> Optional[Feature]:
        return cls._features.get(name)
    
    @classmethod
    def get_all_features(cls) -> Dict[str, Feature]:
        return cls._features.copy()
    
    @classmethod
    def get_dependent_features(cls, feature_names: List[str]) -> Dict[str, Feature]:
        """
        Get all features that the given features depend on, including the features themselves.
        
        Args:
            feature_names: List of feature names to get dependencies for
            
        Returns:
            Dictionary mapping feature names to Feature objects
        """
        result: Dict[str, Feature] = {}
        visited: Set[str] = set()
        all_features = cls.get_all_features()
        
        def collect_dependencies(name: str) -> None:
            """Recursively collect dependencies for a feature."""
            if name in visited:
                return
            
            visited.add(name)
            feature = all_features.get(name)
            if not feature:
                raise ValueError(f"Feature '{name}' not found in repository")
            
            result[name] = feature
            
            # Collect dependencies
            for dep in feature.dep_args:
                dep_name = dep.get_name()
                collect_dependencies(dep_name)
        
        # Collect dependencies for all requested features
        for name in feature_names:
            collect_dependencies(name)
        
        return result
    
    @classmethod
    def clear(cls) -> None:
        cls._features.clear()