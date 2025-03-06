import unittest
from volga.streaming.api.collector.collector import DummyCollector
from volga.streaming.api.context.runtime_context import RuntimeContext
from volga.streaming.api.function.function import SimpleMapFunction
from volga.streaming.api.message.message import Record
from volga.streaming.api.operators.chained import ChainedOneInputOperator
from volga.streaming.api.operators.operators import MapOperator


class TestChaining:

    def test(self):
        map_1 = MapOperator(map_func=SimpleMapFunction(func=(lambda x: x + 2)))
        map_2 = MapOperator(map_func=SimpleMapFunction(func=(lambda x: x + 3)))
        map_3 = MapOperator(map_func=SimpleMapFunction(func=(lambda x: x + 4)))
        ops = [map_1, map_2, map_3]
        coll_lists = []

        for i in range(len(ops)):
            l = []
            coll_lists.append(l)
            collector = DummyCollector(l)
            op = ops[i]
            op.id = i
            op.open(collectors=[collector], runtime_context=RuntimeContext(
                task_id=1,
                task_index=i,
                parallelism=1,
                operator_id=i,
                operator_name=op.__class__.__name__,
            ))

        inp = 10
        for i in range(len(ops)):
            op = ops[i]
            if i == 0:
                _in = inp
            else:
                _in = coll_lists[i - 1][0].value
            op.process_element(Record(_in))

        res = coll_lists[len(ops) - 1][0].value

        out = []
        ch = ChainedOneInputOperator(operators=ops)
        ch.id = 0
        ch.open(collectors=[DummyCollector(out)], runtime_context=RuntimeContext(
                task_id=1,
                task_index=0,
                parallelism=1,
                operator_id=ch.id,
                operator_name=ch.__class__.__name__,
            ))

        ch.process_element(Record(inp))
        ch_res = out[0].value
        assert res == ch_res

        print('assert ok')


if __name__ == '__main__':
    unittest.main()
    # t = TestChaining()
    # t.test()


