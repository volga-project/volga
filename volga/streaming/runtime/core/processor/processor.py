from abc import ABC, abstractmethod
from typing import List

from ray.actor import ActorHandle

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.message.message import Record
from volga.streaming.api.operators.operators import OneInputOperator, Operator, SourceOperator, \
    StreamOperator, OperatorType, TwoInputOperator, ISourceOperator


class Processor(ABC):

    @abstractmethod
    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        pass

    @abstractmethod
    def process(self, record: Record):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def finish(self):
        pass

    @classmethod
    def build_processor(cls, stream_operator: StreamOperator) -> 'StreamProcessor':
        op_type = stream_operator.operator_type()
        if op_type == OperatorType.SOURCE:
            assert isinstance(stream_operator,  ISourceOperator)
            return SourceProcessor(stream_operator)
        elif op_type == OperatorType.ONE_INPUT:
            assert isinstance(stream_operator,  OneInputOperator)
            return OneInputProcessor(stream_operator)
        elif op_type == OperatorType.TWO_INPUT:
            assert isinstance(stream_operator,  TwoInputOperator)
            return TwoInputProcessor(stream_operator)
        else:
            raise RuntimeError('Unsupported operator type')


class StreamProcessor(Processor, ABC):

    def __init__(self, operator: Operator):
        self.operator = operator
        self.collectors = None
        self.runtime_context = None

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        self.collectors = collectors
        self.runtime_context = runtime_context
        self.operator.open(collectors=collectors, runtime_context=runtime_context)

    def close(self):
        self.operator.close()

    def finish(self):
        self.operator.finish()


class OneInputProcessor(StreamProcessor):
    def __init__(self, one_input_operator: OneInputOperator):
        super().__init__(operator=one_input_operator)

    def process(self, record: Record):
        self.operator.process_element(record)


class TwoInputProcessor(StreamProcessor):
    def __init__(self, two_input_operator: TwoInputOperator):
        super().__init__(operator=two_input_operator)
        self.left_stream_name = None
        self.right_stream_name = None

    def process(self, record: Record):
        stream_name = record.stream_name
        if self.left_stream_name == stream_name:
            self.operator.process_element(record, None)
        elif self.right_stream_name == stream_name:
            self.operator.process_element(None, record)
        else:
            raise RuntimeError(f'Unknown stream {stream_name}, left {self.left_stream_name}, right {self.right_stream_name}')


class SourceProcessor(StreamProcessor):
    def __init__(self, source_operator: ISourceOperator):
        super().__init__(operator=source_operator)

    def process(self, record: Record):
        self.operator.fetch()

    def set_master_handle(self, job_master: ActorHandle):
        assert isinstance(self.operator, ISourceOperator)
        source_context = self.operator.get_source_context()
        source_context.set_master_handle(job_master)
