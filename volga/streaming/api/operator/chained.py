from abc import ABC
from typing import List

from volga.streaming.api.collector.collector import Collector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.message.message import Record
from volga.streaming.api.operator.operator import Operator, StreamOperator, OperatorType, OneInputOperator, \
    TwoInputOperator, SourceOperator


class ChainedOperator(Operator, ABC):

    def __init__(self, operators: List[StreamOperator]):
        assert len(operators) > 1
        self.operators = operators
        self.head_operator = operators[0]
        self.tail_operators = gen_tail_operators(self.operators)

    def open(self, collectors: List[Collector], runtime_context: RuntimeContext):
        # do not call super class, we open each operator separately
        for i in range(len(self.operators)):
            operator = self.operators[i]
            succeeding_collectors = []
            for sub_operator in operator.get_next_operators():
                assert isinstance(sub_operator, StreamOperator)
                if sub_operator in self.operators:
                    assert isinstance(sub_operator, OneInputOperator)
                    succeeding_collectors.append(ForwardCollector(sub_operator))
                else:
                    for collector in collectors:
                        if collector.get_id() == operator.id and collector.get_downstream_op_id() == sub_operator.id:
                            succeeding_collectors.append(collector)

            operator.open(succeeding_collectors, self._create_runtime_context(runtime_context, i))

    def finish(self):
        for op in self.operators:
            op.finish()

    def close(self):
        for op in self.operators:
            op.close()

    def operator_type(self) -> OperatorType:
        return self.head_operator.operator_type()

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


class ChainedSourceOperator(ChainedOperator, SourceOperator):

    def __init__(self, operators: List[StreamOperator]):
        super().__init__(operators)
        assert isinstance(self.head_operator, SourceOperator)

    def fetch(self):
        self.head_operator.fetch(self.source_context)


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

#  Find all tail operators of the given operator set. The tail operator contains: 1. The operator
#  that does not have any succeeding operators; 2. The operator whose succeeding operators are all
#  outsiders, i.e. not in the given set.
#
#  @param operators The given operator set
#  @return The tail operators of the given set
def gen_tail_operators(operators: List[StreamOperator]) -> List[StreamOperator]:
    # TODO
    raise
