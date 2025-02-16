import inspect
from typing import List, Callable, Type, Any
from functools import wraps

from volga.api.entity import Entity, validate_decorated_entity
from volga.api.feature import Feature, FeatureRepository, validate_dependencies

class PipelineFeature(Feature):
    def __init__(
        self,
        func: Callable,
        dependencies: List[str],
        output_type: Type,
        is_source: bool = False
    ):
        super().__init__(func, dependencies, output_type)
        self.is_source = is_source


def create_and_register_pipeline_feature(
    func: Callable,
    feature_name: str,
    dependencies: List[str],
    output_type: Type,
    is_source: bool = False
) -> PipelineFeature:
    """Create and register a pipeline feature"""
    # Validate dependencies
    validate_dependencies(feature_name, dependencies)
    
    # Create pipeline feature
    feature = PipelineFeature(
        func=func,
        dependencies=dependencies,
        output_type=output_type,
        is_source=is_source
    )
    
    # Register in FeatureRepository
    FeatureRepository.register(feature)

    if not hasattr(output_type._entity, '_pipelines'):
        output_type._entity._pipelines = {}
        
    if feature_name in output_type._entity._pipelines:
        raise ValueError(f'PipelineFeature {feature_name} already exists')
    output_type._entity._pipelines[feature_name] = feature
    
    return feature

def validate_pipeline_dependencies(
    feature_name: str,
    dependencies: List[str],
    input_params: List[inspect.Parameter]
) -> None:
    """Validate pipeline dependencies and their types"""
    for param, dep_name in zip(input_params, dependencies):
        if param.annotation == inspect.Parameter.empty:
            raise TypeError(
                f'All parameters in function {feature_name} must have type annotations'
            )
        
        # validate_decorated_entity(param.annotation, f'Parameter {param.name}', feature_name)
        
        # Get dependency feature and validate it's a pipeline feature
        dep_feature = FeatureRepository.get_feature(dep_name)
        if dep_feature is None:
            raise ValueError(f'Dependency {dep_name} not found for feature {feature_name}')
        
        if not isinstance(dep_feature, PipelineFeature):
            raise TypeError(
                f'Pipeline feature {feature_name} can only depend on other pipeline features. '
                f'Dependency {dep_name} is {type(dep_feature).__name__}'
            )

# # decorator
# def pipeline(
#     inputs: List[type],
#     output: type
# ) -> Callable:
#     def wrapper(pipeline_func: Callable) -> Callable:
#         if not callable(pipeline_func):
#             raise TypeError('pipeline functions must be callable')
        
#         pipeline_name = pipeline_func.__name__
#         sig = inspect.signature(pipeline_func)
        
#         # Check if it's a class method
#         has_cls_param = False
#         for name, param in sig.parameters.items():
#             if not has_cls_param and param.name != 'cls':
#                 raise TypeError('pipeline functions should be class methods')
#             break
        
#         # Validate input classes are decorated with @entity
#         for inp in inputs:
#             if not hasattr(inp, '_entity') or not isinstance(inp._entity, Entity):
#                 raise TypeError(
#                     f'Input class {inp.__name__} must be decorated with @entity in pipeline {pipeline_name}'
#                 )
        
#         # Validate output class is decorated with @entity
#         if not hasattr(output, '_entity') or not isinstance(output._entity, Entity):
#             raise TypeError(
#                 f'Output class {output.__name__} must be decorated with @entity in pipeline {pipeline_name}'
#             )
        
#         # Create pipeline object
#         pipeline_obj = PipelineFeature(
#             inputs=[inp._entity for inp in inputs],
#             func=pipeline_func,
#         )
        
#         # Register pipeline in the output class's _pipelines dict
#         if not hasattr(output._entity, '_pipelines'):
#             output._entity._pipelines = {}
            
#         if pipeline_name in output._entity._pipelines:
#             raise ValueError(f'Pipeline {pipeline_name} already exists')
#         output._entity._pipelines[pipeline_name] = pipeline_obj
        
#         # Store pipeline object on the function itself
#         setattr(pipeline_func, PIPELINE_ATTR, pipeline_obj)
        
#         return pipeline_func

#     return wrapper


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
        
        # Check parameters match dependencies
        if len(params) != len(dependencies):
            raise TypeError(
                f'Pipeline function {feature_name} has {len(params)} parameters '
                f'but {len(dependencies)} dependencies were specified'
            )
        
        # Validate dependencies and their types
        validate_pipeline_dependencies(feature_name, dependencies, params)
        
        # Create pipeline feature
        feature = create_and_register_pipeline_feature(
            func=pipeline_func,
            feature_name=feature_name,
            dependencies=dependencies,
            output_type=output,
            is_source=False
        )
        
        # Store pipeline object on the function itself
        # TODO: why is this needed?
        # setattr(pipeline_func, PIPELINE_ATTR, feature)
        
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
