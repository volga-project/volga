import inspect
from typing import List, Callable

from volga.api.consts import PIPELINE_ATTR
from volga.api.dataset.dataset import Dataset


class Pipeline:
    inputs: List[Dataset]
    func: Callable
    name: str

    def __init__(
        self,
        inputs: List[Dataset],
        func: Callable,
    ):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__


# decorator
def pipeline(
    inputs: List[Dataset]
) -> Callable:
    def wrapper(pipeline_func: Callable) -> Callable:
        if not callable(pipeline_func):
            raise TypeError('pipeline functions must be callable')
        pipeline_name = pipeline_func.__name__
        sig = inspect.signature(pipeline_func)
        has_cls_param = False
        for name, param in sig.parameters.items():
            if not has_cls_param and param.name != 'cls':
                raise TypeError('pipeline functions should be class methods')
            break
        for inp in inputs:
            if not isinstance(inp, Dataset):
                raise TypeError(
                    f'Parameter {inp.__name__} is not a Dataset in {pipeline_name}'
                )

        setattr(
            pipeline_func,
            PIPELINE_ATTR,
            Pipeline(
                inputs=inputs,
                func=pipeline_func,
            )
        )
        return pipeline_func

    return wrapper
