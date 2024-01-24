from abc import ABC
from typing import Optional

from volga.streaming.api.operator.operator import StreamOperator
from volga.streaming.api.partition.partition import Partition, ForwardPartition


class Stream(ABC):

    def __init__(
        self,
        stream_operator: StreamOperator,
        input_stream: Optional['Stream'] = None,
        streaming_context: Optional['StreamingContext'] = None,
        partition: Optional[Partition] = None
    ):
        if input_stream is None and streaming_context is None:
            raise RuntimeError('input_stream and streaming_context are both None')
        self.stream_operator = stream_operator
        if input_stream is not None:
            self.input_stream = input_stream
        else:
            self.input_stream = None
        if streaming_context is None:
            self.streaming_context = input_stream.streaming_context
        else:
            self.streaming_context = streaming_context

        self.id = self.streaming_context.generate_id()
        self.name = f'{self.id}_{self.__class__.__name__}'

        if self.input_stream is not None:
            self.parallelism = self.input_stream.parallelism
        else:
            self.parallelism = 1

        if partition is None:
            self.partition = ForwardPartition()
        else:
            self.partition = partition

    def set_parallelism(self, parallelism: int) -> 'Stream':
        self.parallelism = parallelism
        return self
