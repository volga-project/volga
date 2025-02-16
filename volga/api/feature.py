from abc import ABC
from typing import Callable, List, Type, Optional, Any, Dict

class Feature(ABC):
    def __init__(
        self,
        func: Callable,
        dependencies: List[str],
        output_type: Type
    ):
        self.func = func
        self.name = func.__name__
        self.output_type = output_type
        self._dependencies: Optional[List['Feature']] = None
        self._dependency_names = dependencies

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    @property
    def dependencies(self) -> List['Feature']:
        if self._dependencies is None:
            self._dependencies = []
            for dep_name in self._dependency_names:
                dep = FeatureRepository.get_feature(dep_name)
                if dep is None:
                    raise ValueError(f'Dependency {dep_name} not found for feature {self.name}')
                self._dependencies.append(dep)
        return self._dependencies

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
    def clear(cls) -> None:
        cls._features.clear()

def validate_dependencies(feature_name: str, dependencies: List[str]) -> None:
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
                'Make sure all dependencies are decorated with either @pipeline or @on_demand'
            )
            
        for dep_name in feat._dependency_names:
            check_circular(dep_name, path)
            
        path.pop()
    
    for dep in dependencies:
        check_circular(dep, [])