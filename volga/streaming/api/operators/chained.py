from abc import ABC
from typing import List, Any

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SourceContext
from volga.streaming.api.message.message import Record
from volga.streaming.api.operators.operators import StreamOperator, OperatorType, OneInputOperator, \
    TwoInputOperator, SourceOperator, ISourceOperator


class ChainedOperator(StreamOperator, ABC):

    def __init__(self, operators: List[StreamOperator]):
        super().__init__(None)
        assert len(operators) > 1
        self.operators = operators
        self.head_operator = operators[0]
        self.tail_operator = operators[len(operators) - 1]

        # set name
        name = f'{self.__class__.__name__}('
        for op in self.operators:
            name += f'{op.get_name()}->'
        self.name = name.removesuffix('->') + ')'

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        # do not call super class, we open each operator separately
        for i in range(len(self.operators) - 1):
            operator = self.operators[i]
            next_operator = self.operators[i + 1]
            assert isinstance(next_operator, OneInputOperator)
            forward_collector = ForwardCollector(next_operator)
            operator.open([forward_collector], self._create_runtime_context(runtime_context, i))

        self.tail_operator.open(collectors, self._create_runtime_context(runtime_context, len(self.operators) - 1))

    def finish(self):
        for op in self.operators:
            op.finish()

    def close(self):
        for op in self.operators:
            op.close()

    def operator_type(self) -> OperatorType:
        return self.head_operator.operator_type()
    
    def set_name(self, name: str):
        raise NotImplementedError('ChainedOperator does not support setting name')

    def get_name(self) -> str:
        return self.name

    def _create_runtime_context(self, runtime_context: RuntimeContext, index: int) -> RuntimeContext:
        # TODO implement based on index
        return runtime_context


class ChainedOneInputOperator(ChainedOperator, OneInputOperator):

    def __init__(self, operators: List[StreamOperator]):
        super().__init__(operators)
        assert isinstance(self.head_operator, OneInputOperator)

    def process_element(self, record: Record):
        self.head_operator.process_element(record)


class ChainedTwoInputOperator(ChainedOperator, TwoInputOperator):

    def __init__(self, operators: List[StreamOperator]):
        super().__init__(operators)
        assert isinstance(self.head_operator, TwoInputOperator)

    def process_element(self, left: Record, right: Record):
        self.head_operator.process_element(left, right)


class ChainedSourceOperator(ChainedOperator, ISourceOperator):

    def __init__(self, operators: List[StreamOperator]):
        assert isinstance(operators[0], SourceOperator)
        super().__init__(operators)

    def fetch(self):
        self.head_operator.fetch()

    def get_source_context(self) -> SourceContext:
        return self.head_operator.get_source_context()

    def get_num_sent(self) -> Any:
        return self.head_operator.get_num_sent()



class ForwardCollector(Collector):

    def __init__(self, succeeding_operator: OneInputOperator):
        self.succeeding_operator = succeeding_operator

    def collect(self, record: Record):
        self.succeeding_operator.process_element(record)


def new_chained_operator(operators: List[StreamOperator]) -> ChainedOperator:
    if operators[0].operator_type() == OperatorType.SOURCE:
        return ChainedSourceOperator(operators)
    elif operators[0].operator_type() == OperatorType.ONE_INPUT:
        return ChainedOneInputOperator(operators)
    elif operators[0].operator_type() == OperatorType.TWO_INPUT:
        return ChainedTwoInputOperator(operators)
    else:
        raise RuntimeError('Unknown operator type')
