import inspect
from typing import List, Callable, Union

from volga.data.api.consts import PIPELINE_ATTR
from volga.data.api.dataset.dataset import Dataset
from volga.data.api.dataset.node import Node


class Pipeline:
    terminal_node: Node
    inputs: List['Dataset']
    _nodes: List
    # Dataset it is part of
    _dataset_name: str
    func: Callable
    name: str

    def __init__(
        self,
        inputs: List['Dataset'],
        func: Callable,
    ):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__


# decorator
def pipeline(
    inputs: List['Dataset']
) -> Callable:
    def wrapper(pipeline_func: Callable) -> Pipeline:
        if not callable(pipeline_func):
            raise TypeError('pipeline functions must be callable')
        pipeline_name = pipeline_func.__name__
        sig = inspect.signature(pipeline_func)
        cls_param = False
        params = []
        for name, param in sig.parameters.items():
            if not cls_param and param.name != 'cls':
                raise TypeError('pipeline functions should be class methods')
            break
        for inp in inputs:
            if not isinstance(inp, Dataset):
                if issubclass(inp, Dataset):
                    raise TypeError('only Dataset as a parameter')
                raise TypeError(
                    f'Parameter {inp.__name__} is not a Dataset in {pipeline_name}'
                )
            if inp.is_terminal:
                raise TypeError(f'cannot have terminal dataset {inp.__name__} as input')
            params.append(inp)

        setattr(
            pipeline_func,
            PIPELINE_ATTR,
            Pipeline(
                inputs=list(params),
                func=pipeline_func,
            )
        )
        return pipeline_func

    return wrapper