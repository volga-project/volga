from typing import Callable, Type, List, Any, Dict, Optional, get_type_hints, Union, Tuple, get_origin, get_args
from volga.api.feature import Feature
from volga.api.pipeline import PipelineFeature
from volga.api.feature import FeatureRepository, validate_dependencies, DepArg
from volga.api.entity import validate_decorated_entity, is_entity_type
import inspect
from functools import wraps

class OnDemandFeature(Feature):
    def __init__(
        self,
        func: Callable,
        dep_args: List[DepArg],
        output_type: Type
    ):
        super().__init__(func, dep_args, output_type)

        # Store query names for pipeline dependencies
        self.query_names: Dict[str, str] = {
            dep_arg.get_name(): dep_arg.query_name
            for dep_arg in dep_args
            if dep_arg.query_name is not None
        }

        # Get function signature and type hints
        sig = inspect.signature(func)
        type_hints = get_type_hints(func)

        # Get UDF argument names, excluding entity types
        self.udf_args_names = []
        for name, param in sig.parameters.items():
            # Skip entity types and lists of entities
            if name in type_hints and is_entity_type(type_hints[name]):
                continue
            # Include parameter if it has a default value
            if param.default != param.empty:
                self.udf_args_names.append(name)

    def execute(
        self,
        dep_values: List[Any],
        udf_args: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Execute feature with dependencies and UDF arguments"""
        # Add UDF arguments if provided
        if udf_args is None:
            udf_args = {}
            
        # Validate all provided UDF args are valid
        if self.udf_args_names:
            invalid_args = set(udf_args.keys()) - set(self.udf_args_names)
            if invalid_args:
                raise ValueError(
                    f"Invalid UDF arguments for feature {self.name}: {invalid_args}"
                )
        elif udf_args:
            raise ValueError(f"UDF args provided for feature {self.name} that takes no arguments")
            
        # Execute UDF
        return self.func(*dep_values, **udf_args)

def on_demand(dependencies: List[Union[str, Tuple[str, str]]]) -> Callable:
    """
    Decorator for on-demand feature functions.
    
    Args:
        dependencies: List of dependencies, each can be:
            - str: feature name for simple dependency
            - Tuple[str, str]: (feature_name, query_name) for pipeline features
    """
    def wrapper(func: Callable) -> Callable:
        if not callable(func):
            raise TypeError('on_demand functions must be callable')
        
        feature_name = func.__name__
        
        # Convert dependencies to DepArg
        dep_args = [
            DepArg(feature_name=dep[0], query_name=dep[1]) if isinstance(dep, tuple)
            else DepArg(feature_name=dep)
            for dep in dependencies
        ]

        validate_dependencies(feature_name, dep_args)
        
        # Get function signature and parameters
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        type_hints = get_type_hints(func)
        
        # Validate input types
        for param, dep_arg in zip(params, dep_args):
            if param.name not in type_hints:
                raise TypeError(
                    f'Parameter {param.name} in on_demand function {feature_name} '
                    'must have a type annotation'
                )
            
            param_type = type_hints[param.name]
            
            # Check if parameter is a List type
            is_list = get_origin(param_type) is list
            if is_list:
                entity_type = get_args(param_type)[0]
                validate_decorated_entity(entity_type, f'List parameter {param.name}', feature_name)
            else:
                validate_decorated_entity(param_type, f'Parameter {param.name}', feature_name)
            
            # Get dependency feature and validate type compatibility
            dep_feature = FeatureRepository.get_feature(dep_arg.get_name())
            if dep_feature is None:
                raise ValueError(f'Dependency {dep_arg.get_name()} not found for feature {feature_name}')
            
            # Validate type compatibility
            expected_type = get_args(param_type)[0] if is_list else param_type
            if expected_type != dep_feature.output_type:
                raise TypeError(
                    f'Parameter {param.name} type {expected_type.__name__} does not match '
                    f'dependency {dep_arg.get_name()} output type {dep_feature.output_type.__name__}'
                )

        # Get return type annotation
        return_type = sig.return_annotation
        if return_type == inspect.Parameter.empty:
            raise TypeError(
                f'on_demand function {feature_name} must have a return type annotation'
            )
        
        validate_decorated_entity(return_type, 'Return', feature_name)
        
        # Create on-demand feature
        feature = OnDemandFeature(
            func=func,
            dep_args=dep_args,
            output_type=return_type
        )

        # Register with entity metadata
        return_type._entity_metadata.register_on_demand_feature(feature_name, feature)
    
        # Register in FeatureRepository
        FeatureRepository.register(feature)
        
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            return feature.execute(args, kwargs)
        
        return wrapped_func
    
    return wrapper