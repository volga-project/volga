import functools
from typing import Callable, Dict, Type, List, Optional, Any

from volga.common.time_utils import is_time_str
from volga.data.api.dataset.aggregate import AggregateType
from volga.data.api.dataset.schema import DatasetSchema
from volga.streaming.api.message.message import Record
from volga.streaming.api.operator.window_operator import SlidingWindowConfig, AggregationsPerWindow
from volga.streaming.api.stream.data_stream import DataStream, KeyDataStream


class OperatorNodeBase:
    def __init__(self):
        self.stream: Optional[DataStream] = None
        self.parents: List['OperatorNodeBase'] = []

    def init_stream(self, *args):
        raise NotImplementedError()


# user facing operators to construct pipeline graph
class OperatorNode(OperatorNodeBase):

    def transform(self, func: Callable) -> 'OperatorNode':
        return Transform(self, func)

    def filter(self, func: Callable) -> 'OperatorNode':
        return Filter(self, func)

    def assign(self, column: str, result_type: Type, func: Callable) -> 'OperatorNode':
        return Assign(self, column, result_type, func)

    def group_by(self, keys: List[str]) -> 'GroupBy':
        return GroupBy(self, keys)

    def join(
        self,
        other: 'Dataset',
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ) -> 'OperatorNode':
        if on is None:
            if left_on is None or right_on is None:
                raise TypeError("Join should provide either on or both left_on and right_on")

        if left_on is None or right_on is None:
            if on is None:
                raise TypeError("Join should provide either on or both left_on and right_on")

        return Join(left=self, right=other, on=on, left_on=left_on, right_on=right_on)

    def rename(self, columns: Dict[str, str]) -> 'OperatorNode':
        return Rename(self, columns)

    def drop(self, columns: List[str]) -> 'OperatorNode':
        return Drop(self, columns, name="drop")

    def dropnull(self, columns: Optional[List[str]] = None) -> 'OperatorNode':
        return DropNull(self, columns)

    def select(self, columns: List[str]) -> 'OperatorNode':
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


class Transform(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, func: Callable):
        super().__init__()
        self.func = func
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        return self.func(event)


class Assign(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, column: str, output_type: Type, func: Callable):
        super().__init__()
        self.parents.append(parent)
        self.func = func
        self.column = column
        self.output_type = output_type

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        raise NotImplementedError()


class Filter(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, func: Callable):
        super().__init__()

        self.parents.append(parent)
        self.func = func

    def init_stream(self):
        self.stream = self.parents[0].stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> bool:
        return self.func(event)


class Aggregate(OperatorNode):
    def __init__(
        self, parent: OperatorNodeBase, aggregates: List[AggregateType]
    ):
        super().__init__()
        self.aggregates = aggregates
        self.parents.append(parent)

    def init_stream(self, target_dataset_schema: DatasetSchema):

        def _output_window_func(aggs_per_window: AggregationsPerWindow, record: Record) -> Record:
            record_value = record.value
            res = {}

            # TODO this is a hacky way to infer timestamp field from the record
            #  Ideally we need to build resulting schema for each node at compilation time and use it to
            #  derive field names/validate correctness

            # copy keys
            expected_key_fields = list(target_dataset_schema.keys.keys())
            for k in expected_key_fields:
                if k not in record_value:
                    raise RuntimeError(f'Can not locate key field {k}')
                res[k] = record_value[k]

            # copy timestamp
            ts_field = target_dataset_schema.timestamp
            ts_value = None
            for v in record_value.values():
                if is_time_str(v):
                    ts_value = v
                    break

            if ts_value is None:
                raise RuntimeError(f'Unable to locate timestamp field: {record_value}')
            res[ts_field] = ts_value

            # copy aggregate values
            values_fields = list(target_dataset_schema.values.keys())
            for v in values_fields:
                if v not in aggs_per_window:
                    raise RuntimeError(f'Unable to locate {v} in aggregates: {aggs_per_window}')
                res[v] = aggs_per_window[v]

            res_record = Record(value=res, event_time=record.event_time)
            res_record.set_stream_name(record.stream_name)
            return res_record

        parent = self.parents[0].stream
        assert isinstance(parent, KeyDataStream)
        self.stream = parent.multi_window_agg(
            configs=self._stream_window_aggregate_configs(),
            output_func=_output_window_func
        )

    def _stream_window_aggregate_configs(self) -> List[SlidingWindowConfig]:
        return [SlidingWindowConfig(
            duration=agg.window,
            agg_type=agg.get_type(),
            agg_on_func=functools.partial(lambda e, key: e[key], key=agg.on),
            name=agg.into
        ) for agg in self.aggregates]


class GroupBy(OperatorNodeBase):
    def __init__(self, parent: OperatorNodeBase, keys: List[str]):
        super().__init__()
        # TODO support composite key
        if len(keys) != 1:
            raise ValueError('Currently GroupBy expects exactly 1 key field')
        self.keys = keys
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.key_by(key_by_func=self._stream_key_by_func)

    def _stream_key_by_func(self, event: Any) -> Any:
        assert isinstance(event, Dict)
        key = self.keys[0]
        if key not in event:
            raise RuntimeError(f'key {key} not in event {event}')

        return event[key]

    def aggregate(self, aggregates: List[AggregateType]) -> OperatorNode:
        if len(aggregates) == 0:
            raise ValueError('Aggregate expects at least one aggregation operation')
        return Aggregate(self, aggregates)


class Join(OperatorNode):
    def __init__(
        self,
        left: OperatorNode,
        right: 'Dataset',
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None
    ):
        super().__init__()
        self.left = left
        self.right = right

        self.parents.append(left)
        self.parents.append(right)
        self.on = on

        # TODO support composite keys
        if on is not None and len(on) != 1 or \
                left_on is not None and len(left_on) != 1 or \
                right_on is not None and len(right_on) != 1:
            raise ValueError('Currently, Join expects exactly 1 key field')

        self.left_on = left_on
        self.right_on = right_on

    def init_stream(self):
        self.stream = self.left.stream.key_by(self._stream_left_key_func) \
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

    # TODO cast to target_dataset_schema in case of join being terminal node
    def _stream_join_func(self, left: Any, right: Any) -> Any:
        if left is None or right is None:
            raise RuntimeError('Can not join null values')
        assert isinstance(left, Dict)
        assert isinstance(right, Dict)

        # TODO we can compute same keys and resulting output schema only once to increase perf
        same_keys = list(set(left.keys()) & set(right.keys()))

        # rename with prefixex
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


class Rename(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: Dict[str, str]):
        super().__init__()
        self.column_mapping = columns
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        raise NotImplementedError()


class Drop(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: List[str], name="drop"):
        super().__init__()
        self.parents.append(parent)
        self.columns = columns
        self.__name = name

    def init_stream(self):
        self.stream = self.parents[0].stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> Any:
        raise NotImplementedError()


class DropNull(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: Optional[List[str]] = None):
        super().__init__()
        self.columns = columns
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.filter(filter_func=self._stream_filter_func)

    def _stream_filter_func(self, event: Any) -> Any:
        raise NotImplementedError()
