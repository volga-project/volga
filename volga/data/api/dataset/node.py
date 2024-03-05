import copy
from typing import Callable, Dict, Type, List, Optional, Any

from volga.data.api.dataset.aggregate import AggregateType
from volga.data.api.dataset.schema import DataSetSchema
from volga.streaming.api.operator.window_operator import SlidingWindowConfig
from volga.streaming.api.stream.data_stream import DataStream, KeyDataStream


# user facing operators to construct pipeline graph
class Node:
    parent: 'Node'
    stream: DataStream

    def __init__(self):
        self.out_edges = []

    def data_set_schema(self) -> DataSetSchema:
        raise NotImplementedError()

    def transform(self, func: Callable, schema: Dict = {}) -> 'Node':
        if schema == {}:
            return Transform(self, func, None)
        return Transform(self, func, copy.deepcopy(schema))

    def filter(self, func: Callable) -> 'Node':
        return Filter(self, func)

    def assign(self, column: str, result_type: Type, func: Callable) -> 'Node':
        return Assign(self, column, result_type, func)

    def group_by(self, keys: List[str]) -> 'GroupBy':
        return GroupBy(self, keys)

    def join(
            self,
            other: 'Dataset',
            on: Optional[List[str]] = None,
            left_on: Optional[List[str]] = None,
            right_on: Optional[List[str]] = None,
    ) -> 'Node':
        if on is None:
            if left_on is None or right_on is None:
                raise TypeError("Join should provide either on or both left_on and right_on")

        if left_on is None or right_on is None:
            if on is None:
                raise TypeError("Join should provide either on or both left_on and right_on")

        return Join(self, other, on)

    def rename(self, columns: Dict[str, str]) -> 'Node':
        return Rename(self, columns)

    def drop(self, columns: List[str]) -> 'Node':
        return Drop(self, columns, name="drop")

    def dropnull(self, columns: Optional[List[str]] = None) -> 'Node':
        return DropNull(self, columns)

    def select(self, columns: List[str]) -> 'Node':
        ts = self.data_set_schema().timestamp
        # Keep the timestamp col
        drop_cols = list(
            filter(
                lambda c: c not in columns and c != ts, self.data_set_schema().fields()
            )
        )
        # All the cols were selected
        if len(drop_cols) == 0:
            return self
        return Drop(self, drop_cols, name="select")


class Transform(Node):
    def __init__(self, parent: Node, func: Callable, schema: Optional[Dict]):
        super().__init__()
        self.func = func
        self.parent = parent
        self.parent.out_edges.append(self)
        self.new_schema = schema
        self.stream = self.parent.stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        return self.func(event)


class Assign(Node):
    def __init__(self, parent: Node, column: str, output_type: Type, func: Callable):
        super().__init__()
        self.parent = parent
        self.parent.out_edges.append(self)
        self.func = func
        self.column = column
        self.output_type = output_type
        self.stream = self.parent.stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        raise NotImplementedError()


class Filter(Node):
    def __init__(self, parent: Node, func: Callable):
        super().__init__()
        self.parent = parent
        self.parent.out_edges.append(self)
        self.func = func
        self.stream = self.parent.stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> bool:
        return self.func(event)


class Aggregate(Node):
    def __init__(
            self, parent: Node, keys: List[str], aggregates: List[AggregateType]
    ):
        super().__init__()
        if len(keys) == 0:
            raise ValueError("Must specify at least one key")
        self.keys = keys
        self.aggregates = aggregates
        self.parent = parent
        self.parent.out_edges.append(self)

        assert isinstance(self.parent.stream, KeyDataStream)

        self.stream = self.parent.stream.multi_window_agg(self._stream_window_aggregate_configs())

    def _stream_window_aggregate_configs(self) -> List[SlidingWindowConfig]:
        return [SlidingWindowConfig(
            duration=agg.window,
            agg_type=agg.get_type(),
            agg_on=agg.on,
            name=agg.into
        ) for agg in self.aggregates]


class GroupBy:
    def __init__(self, parent: Node, keys: List[str]):
        # TODO support composite key
        if len(keys) != 1:
            raise ValueError('Currently GroupBy expects exactly 1 key field')
        self.keys = keys
        self.parent = parent
        self.parent.out_edges.append(self)
        self.stream = self.parent.stream.key_by(key_by_func=self._stream_key_by_func)

    def _stream_key_by_func(self, event: Any) -> Any:
        assert isinstance(event, Dict)
        return event[self.keys[0]]

    def aggregate(self, aggregates: List[AggregateType]) -> Node:
        if len(aggregates) == 0:
            raise TypeError(
                "aggregate operator expects atleast one aggregation operation"
            )
        return Aggregate(self.parent, self.keys, aggregates)


class Join(Node):
    def __init__(
            self,
            left: Node,
            right: 'Dataset',
            on: Optional[List[str]] = None,
            left_on: Optional[List[str]] = None,
            right_on: Optional[List[str]] = None
    ):
        super().__init__()
        self.parent = left
        self.right = right
        self.on = on

        # TODO support composite keys
        if on is not None and len(on) != 1 or \
                left_on is not None and len(left_on) != 1 or \
                right_on is not None and len(right_on) != 1:
            raise ValueError('Currently, Join expects exactly 1 key field')

        self.left_on = left_on
        self.right_on = right_on
        self.parent.out_edges.append(self)
        self.stream = self.parent.stream.key_by(self._stream_left_key_func) \
            .join(self.right.stream.key_by(self._stream_right_key_func)) \
            .with_func(self._stream_join_func)

    def _stream_left_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        key = self.left_on[0] if self.on is None else self.on[0]
        return element[key]

    def _stream_right_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        key = self.right_on[0] if self.on is None else self.on[0]
        return element[key]

    def _stream_join_func(self, left: Any, right: Any) -> Any:
        assert isinstance(left, Dict)
        assert isinstance(right, Dict)

        # TODO we can compute same keys and resulting output schema only once to increase perf
        same_keys = list(set(left.keys()) & set(right.keys()))

        # rename
        for k in same_keys:
            if k in left:
                new_k_left = f'left_{k}'
                assert new_k_left not in left
                left[new_k_left] = left[k]
                del left[k]
            if k in right:
                new_k_right = f'right_{k}'
                assert new_k_right not in right
                left[new_k_right] = right[k]
                del right[k]

        return {**left, **right}


class Rename(Node):
    def __init__(self, parent: Node, columns: Dict[str, str]):
        super().__init__()
        self.parent = parent
        self.column_mapping = columns
        self.parent.out_edges.append(self)
        self.stream = self.parent.stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        raise NotImplementedError()


class Drop(Node):
    def __init__(self, parent: Node, columns: List[str], name="drop"):
        super().__init__()
        self.parent = parent
        self.columns = columns
        self.__name = name
        self.parent.out_edges.append(self)
        self.stream = self.parent.stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> Any:
        raise NotImplementedError()


class DropNull(Node):
    def __init__(self, parent: Node, columns: Optional[List[str]] = None):
        super().__init__()
        self.parent = parent
        self.columns = columns
        self.parent.out_edges.append(self)
        self.stream = self.parent.stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> Any:
        raise NotImplementedError()
