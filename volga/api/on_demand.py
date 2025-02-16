from typing import Callable, Type, List
from volga.api.feature import Feature
from volga.api.feature import FeatureRepository, validate_dependencies
from volga.api.entity import validate_decorated_entity
import inspect
from functools import wraps


class OnDemandFeature(Feature):
    def __init__(
        self,
        func: Callable,
        dependencies: List[str],
        output_type: Type
    ):
        super().__init__(func, dependencies, output_type)
    
def on_demand(dependencies: List[str]) -> Callable:
    def wrapper(func: Callable) -> Callable:
        if not callable(func):
            raise TypeError('on_demand functions must be callable')
        
        feature_name = func.__name__
        
        # Get function signature and parameters
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        
        # Check number of parameters matches number of dependencies
        if len(params) != len(dependencies):
            raise TypeError(
                f'On-demand function {feature_name} has {len(params)} parameters '
                f'but {len(dependencies)} dependencies were specified'
            )
        
        # Validate input types
        for param, dep_name in zip(params, dependencies):
            if param.annotation == inspect.Parameter.empty:
                raise TypeError(
                    f'All parameters in on_demand function {feature_name} must have type annotations'
                )
            
            validate_decorated_entity(param.annotation, f'Parameter {param.name}', feature_name)
            
            # Get dependency feature and validate type compatibility
            dep_feature = FeatureRepository.get_feature(dep_name)
            if dep_feature is None:
                raise ValueError(f'Dependency {dep_name} not found for feature {feature_name}')
                
            if dep_feature.output_type != param.annotation:
                raise TypeError(
                    f'Parameter {param.name} type {param.annotation} does not match '
                    f'dependency {dep_name} output type {dep_feature.output_type}'
                )

        # Get return type annotation
        return_type = sig.return_annotation
        if return_type == inspect.Parameter.empty:
            raise TypeError(
                f'on_demand function {feature_name} must have a return type annotation'
            )
        
        validate_decorated_entity(return_type, 'Return', feature_name)
        
        # Validate dependencies
        validate_dependencies(feature_name, dependencies)
        
        # Create on-demand feature
        feature = OnDemandFeature(
            func=func,
            dependencies=dependencies,
            output_type=return_type
        )

        if not hasattr(return_type._entity, '_on_demands'):
            return_type._entity._on_demands = {}
            
        if feature_name in return_type._entity._on_demands:
            raise ValueError(f'OnDemandFeature {feature_name} already exists')
        return_type._entity._on_demands[feature_name] = feature
        
        # Register in FeatureRepository
        FeatureRepository.register(feature)
        
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            # Validate input arguments are instances of their declared types
            for arg, param in zip(args, params):
                if not isinstance(arg, param.annotation):
                    raise TypeError(
                        f'Argument for parameter {param.name} must be an instance of '
                        f'{param.annotation.__name__}, got {type(arg).__name__} instead'
                    )
            
            # Call the original function
            result = func(*args, **kwargs)
            
            # Validate return value is an instance of the declared output type
            if not isinstance(result, return_type):
                raise TypeError(
                    f'Return value of {feature_name} must be an instance of '
                    f'{return_type.__name__}, got {type(result).__name__} instead'
                )
            
            return result
        
        return wrapped_func
    
    return wrapper