import inspect
from typing import List, Callable

from volga.api.entity.entity import Entity

# Add this constant at the top of the file
PIPELINE_ATTR = '_pipeline_attr'

class Pipeline:
    inputs: List[Entity]
    func: Callable
    name: str

    def __init__(
        self,
        inputs: List[Entity],
        func: Callable,
    ):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__


# decorator
def pipeline(
    inputs: List[type],
    output: type
) -> Callable:
    def wrapper(pipeline_func: Callable) -> Callable:
        if not callable(pipeline_func):
            raise TypeError('pipeline functions must be callable')
        
        pipeline_name = pipeline_func.__name__
        sig = inspect.signature(pipeline_func)
        
        # Check if it's a class method
        has_cls_param = False
        for name, param in sig.parameters.items():
            if not has_cls_param and param.name != 'cls':
                raise TypeError('pipeline functions should be class methods')
            break
        
        # Validate input classes are decorated with @entity
        for inp in inputs:
            if not hasattr(inp, '_entity') or not isinstance(inp._entity, Entity):
                raise TypeError(
                    f'Input class {inp.__name__} must be decorated with @entity in pipeline {pipeline_name}'
                )
        
        # Validate output class is decorated with @entity
        if not hasattr(output, '_entity') or not isinstance(output._entity, Entity):
            raise TypeError(
                f'Output class {output.__name__} must be decorated with @entity in pipeline {pipeline_name}'
            )
        
        # Create pipeline object
        pipeline_obj = Pipeline(
            inputs=[inp._entity for inp in inputs],
            func=pipeline_func,
        )
        
        # Register pipeline in the output class's _pipelines dict
        if not hasattr(output._entity, '_pipelines'):
            output._entity._pipelines = {}
            
        if pipeline_name in output._entity._pipelines:
            raise ValueError(f'Pipeline {pipeline_name} already exists')
        output._entity._pipelines[pipeline_name] = pipeline_obj
        
        # Store pipeline object on the function itself
        setattr(pipeline_func, PIPELINE_ATTR, pipeline_obj)
        
        return pipeline_func

    return wrapper
