import inspect
from typing import List, Callable

from volga.data.api.consts import PIPELINE_ATTR
from volga.data.api.dataset.dataset import Dataset
from volga.data.api.dataset.node import Node
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream import Stream
from volga.streaming.api.stream.stream_sink import StreamSink
from volga.streaming.api.stream.stream_source import StreamSource


class Pipeline:
    terminal_node: Node
    inputs: List['Dataset']
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

    def set_terminal_node(self, node: Node):
        if node is None:
            raise Exception(f'Pipeline {self.name} cannot return None')
        self.terminal_node = node

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


def to_stream(pipeline: Pipeline) -> List[StreamSink]:
    res = []

    ctx = StreamingContext()
    # TODO figure transient input Datasets (i.e not @source annotated)
    sources = [to_stream_source(i, ctx) for i in pipeline.inputs]
    terminal_node = pipeline.terminal_node

    return res

def to_stream_source(ds: Dataset, ctx: StreamingContext) -> StreamSource:

    return

def _traverse(node: Node, stream: Stream):

    pass