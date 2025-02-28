import inspect
from typing import List, Callable, Type, Any
from functools import wraps

from volga.api.entity import Entity, validate_decorated_entity
from volga.api.feature import Feature, FeatureRepository, validate_dependencies, DepArg

class PipelineFeature(Feature):
    def __init__(
        self,
        func: Callable,
        dep_args: List[DepArg],
        output_type: Type,
        is_source: bool = False
    ):
        super().__init__(func, dep_args, output_type)
        self.is_source = is_source


def create_and_register_pipeline_feature(
    func: Callable,
    feature_name: str,
    dep_args: List[DepArg],
    output_type: Type,
    is_source: bool = False
) -> PipelineFeature:
    """Create and register a pipeline feature"""
    # Validate dependencies
    validate_dependencies(feature_name, dep_args)
    
    # Create pipeline feature
    feature = PipelineFeature(
        func=func,
        dep_args=dep_args,
        output_type=output_type,
        is_source=is_source
    )
    
    # Register with entity metadata
    output_type._entity_metadata.register_pipeline_feature(feature_name, feature)

    # Register in FeatureRepository
    FeatureRepository.register(feature)
    
    
    return feature

def validate_pipeline_dependencies(
    feature_name: str,
    dep_args: List[DepArg],
    input_params: List[inspect.Parameter]
) -> None:
    """Validate pipeline dependencies and their types"""
    for param, dep_arg in zip(input_params, dep_args):
        if param.annotation == inspect.Parameter.empty:
            raise TypeError(
                f'All parameters in function {feature_name} must have type annotations'
            )
        
        # Get dependency feature and validate it's a pipeline feature
        dep_feature = FeatureRepository.get_feature(dep_arg.get_name())
        if dep_feature is None:
            raise ValueError(f'Dependency {dep_arg.get_name()} not found for feature {feature_name}')
        
        if not isinstance(dep_feature, PipelineFeature):
            raise TypeError(
                f'Pipeline feature {feature_name} can only depend on other pipeline features. '
                f'Dependency {dep_arg.get_name()} is {type(dep_feature).__name__}'
            )


def pipeline(dependencies: List[str], output: Type) -> Callable:
    # Validate output type has @entity decorator
    validate_decorated_entity(output, 'Output', 'pipeline decorator')
    
    def wrapper(pipeline_func: Callable) -> Callable:
        if not callable(pipeline_func):
            raise TypeError('pipeline functions must be callable')
        
        feature_name = pipeline_func.__name__
        
        # Get function signature and parameters
        sig = inspect.signature(pipeline_func)
        params = list(sig.parameters.values())
        
        # Convert dependencies to DepArg
        dep_args = [DepArg(feature_name=dep) for dep in dependencies]
        
        # Check parameters match dependencies
        if len(params) != len(dep_args):
            raise TypeError(
                f'Pipeline function {feature_name} has {len(params)} parameters '
                f'but {len(dep_args)} dependencies were specified'
            )
        
        # Validate dependencies and their types
        validate_pipeline_dependencies(feature_name, dep_args, params)
        
        # Create pipeline feature
        create_and_register_pipeline_feature(
            func=pipeline_func,
            feature_name=feature_name,
            dep_args=dep_args,
            output_type=output,
            is_source=False
        )
        
        @wraps(pipeline_func)
        def wrapped_func(*args, **kwargs):
            # Validate input arguments are Entities
            for arg, param in zip(args, params):
                if not isinstance(arg, Entity):
                    raise TypeError(
                        f'Argument for parameter {param.name} must be an instance of '
                        f'{Entity.__name__}, got {type(arg).__name__} instead'
                    )
                
            # Call the original function
            result = pipeline_func(*args, **kwargs)
            
            # Validate return value is Entity
            if not isinstance(result, Entity):
                raise TypeError(
                    f'Return value of {feature_name} must be an instance of '
                    f'{Entity.__name__}, got {type(result).__name__} instead'
                )
            
            return result
        
        return wrapped_func

    return wrapper
