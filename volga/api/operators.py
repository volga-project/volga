from datetime import datetime
import functools
from typing import Callable, Dict, Type, List, Optional, Any

from volga.common.time_utils import is_time_str
from volga.api.aggregate import AggregateType
from volga.api.schema import Schema
from volga.streaming.api.message.message import Record
from volga.streaming.api.operators.window_operator import SlidingWindowConfig, AggregationsPerWindow
from volga.streaming.api.stream.data_stream import DataStream, KeyDataStream


class OperatorNodeBase:
    def __init__(self):
        self.stream: Optional[DataStream] = None
        self.parents: List['OperatorNodeBase'] = []

    def init_stream(self, *args):
        raise NotImplementedError()

    # TODO we want to be able to cast schema to Dataset's defined schema if the operator is the last in chain
    def schema(self) -> Schema:
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
        other: 'OperatorNode',
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ) -> 'OperatorNode':
        if on is None:
            if left_on is None or right_on is None:
                raise ValueError('Join should provide either on or both left_on and right_on')

        if left_on is None or right_on is None:
            if on is None:
                raise ValueError('Join should provide either on or both left_on and right_on')

        return Join(left=self, right=other, on=on, left_on=left_on, right_on=right_on)

    def rename(self, columns: Dict[str, str]) -> 'OperatorNode':
        return Rename(self, columns)

    def drop(self, columns: List[str]) -> 'OperatorNode':
        return Drop(self, columns)

    def dropnull(self, columns: Optional[List[str]] = None) -> 'OperatorNode':
        return DropNull(self, columns)

    def select(self, columns: List[str]) -> 'OperatorNode':
        ts = self.schema().timestamp
        # Keep the timestamp col
        drop_cols = list(filter(
            lambda c: c not in columns and c != ts, self.schema().fields()
        ))
        # All the cols were selected
        if len(drop_cols) == 0:
            return self
        return Drop(self, drop_cols)


class Transform(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, func: Callable, new_schema_dict: Optional[Dict[str, Type]] = None):
        super().__init__()
        self.func = func
        self.parents.append(parent)
        self.new_schema_dict = new_schema_dict

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        return self.func(event)

    def schema(self) -> Schema:
        input_schema = self.parents[0].schema()
        if self.new_schema_dict is None:
            # schema has not been changed
            return input_schema

        # we assume new schema alters value fields only, keys and timestamp fields are intact
        keys = input_schema.keys
        values = {}
        for field, type in self.new_schema_dict.items():
            if field in keys.keys() or field == input_schema.timestamp:
                continue
            if field in values:
                raise ValueError(f'Duplicate field {field} for Transform operator schema')
            values[field] = type

        return Schema(
            keys=keys,
            values=values,
            timestamp=input_schema.timestamp,
        )


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

    def schema(self) -> Schema:
        # filtering  does not alter parent's schema
        return self.parents[0].schema()


class Aggregate(OperatorNode):
    def __init__(
        self, parent: OperatorNodeBase, aggregates: List[AggregateType]
    ):
        super().__init__()
        self.aggregates = aggregates
        self.parents.append(parent)

    def init_stream(self, output_schema: Schema):

        def _output_window_func(aggs_per_window: AggregationsPerWindow, record: Record) -> Record:
            record_value = record.value
            res = {}

            # TODO this is a hacky way to infer timestamp field from the record
            #  Ideally we need to build resulting schema for each node at compilation time and use it to
            #  derive field names/validate correctness

            # copy keys
            expected_key_fields = list(output_schema.keys.keys())
            for k in expected_key_fields:
                if k not in record_value:
                    raise RuntimeError(f'Can not locate key field {k}')
                res[k] = record_value[k]

            # copy timestamp
            ts_field = output_schema.timestamp
            ts_value = None
            for v in record_value.values():
                if is_time_str(v):
                    ts_value = v
                    break

            if ts_value is None:
                raise RuntimeError(f'Unable to locate timestamp field: {record_value}')
            res[ts_field] = ts_value

            # copy aggregate values
            values_fields = list(output_schema.values.keys())
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

    def schema(self) -> Schema:
        # group by does not alter parent's schema
        return self.parents[0].schema()


class Join(OperatorNode):
    def __init__(
        self,
        left: OperatorNode,
        right: OperatorNode,
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

        if left_on is None and right_on is not None or \
                left_on is not None and right_on is None:
            raise ValueError('Join expects both left_on and right_on')

        self.left_on = left_on
        self.right_on = right_on

        # fields with the same name
        self._same_fields = list(set(self.left.schema().fields()) & set(self.right.schema().fields()))

    def init_stream(self):
        self.stream = self.left.stream.key_by(self._stream_left_key_func) \
            .join(self.right.stream.key_by(self._stream_right_key_func)) \
            .with_func(self._stream_join_func)

    @staticmethod
    def _prefix_duplicate_field(field: str, is_left: bool):
        if is_left:
            return f'left_{field}'
        else:
            return f'right_{field}'

    @staticmethod
    def _joined_schema(ls: Schema, rs: Schema, on: Optional[List[str]], left_on: Optional[List[str]]):
        same_fields = list(set(ls.fields()) & set(rs.fields()))
        ts = ls.timestamp  # we use left ts by default

        keys = {}
        values = {}
        if on is not None:
            for k in on:
                keys[k] = ls.keys[k]
        else:
            # use left keys by default
            for k in left_on:
                keys[k] = ls.keys[k]

        for f in ls.fields():
            if f == ts or f in keys:
                continue
            renamed_f = None
            if f in same_fields:
                renamed_f = Join._prefix_duplicate_field(field=f, is_left=True)
            new_f = f if renamed_f is None else renamed_f
            if new_f in values:
                raise RuntimeError(f'Duplicate entry for filed {new_f}')
            if f in ls.values:
                values[new_f] = ls.values[f]
            elif f in ls.keys:
                values[new_f] = ls.keys[f]
            else:
                raise ValueError(f'Unable to locate field {f} in left side of a join')

        for f in rs.fields():
            if f == ts or f in keys:
                continue
            renamed_f = None
            if f in same_fields:
                renamed_f = Join._prefix_duplicate_field(field=f, is_left=False)
            new_f = f if renamed_f is None else renamed_f
            if new_f in values:
                raise RuntimeError(f'Duplicate entry for filed {new_f}')
            if f in rs.values:
                values[new_f] = rs.values[f]
            elif f in rs.keys:
                values[new_f] = rs.keys[f]
            elif f == rs.timestamp:
                # right still has a timestamp field, we need to handle it explicitly
                values[new_f] = datetime
            else:
                raise ValueError(f'Unable to locate field {f} in left side of a join')

        return Schema(
            keys=keys,
            values=values,
            timestamp=ts
        )

    def schema(self) -> Schema:
        return Join._joined_schema(self.left.schema(), self.right.schema(), self.on, self.left_on)

    def _stream_left_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        key = self.left_on[0] if self.on is None else self.on[0]
        return element[key]

    def _stream_right_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        key = self.right_on[0] if self.on is None else self.on[0]
        return element[key]

    # {'buyer_id': '0', 'product_type': 'ON_SALE', 'purchased_at': '2024-05-07 14:08:26.519626', 'product_price': 100.0, 'name': 'username_0'}
    # {'buyer_id': '0', 'product_id': 'prod_0', 'product_type': 'ON_SALE', 'purchased_at': '2024-05-07 14:14:20.335705', 'product_price': 100.0, 'user_id': '0', 'registered_at': '2024-05-07 14:14:20.335697', 'name': 'username_0'}
    def _stream_join_func(self, left: Any, right: Any) -> Any:
        for k in self._same_fields:
            if k in left:
                new_k_left = Join._prefix_duplicate_field(field=k, is_left=True)
                assert new_k_left not in left
                left[new_k_left] = left[k]
                del left[k]
            if k in right:
                new_k_right = Join._prefix_duplicate_field(field=k, is_left=False)
                assert new_k_right not in right
                left[new_k_right] = right[k]
                del right[k]

        return {**left, **right}

    # TODO cast to target_dataset_schema in case of join being terminal node
    def _stream_join_func_new(self, left: Any, right: Any) -> Any:
        if left is None or right is None:
            raise RuntimeError('Can not join null values')
        assert isinstance(left, Dict)
        assert isinstance(right, Dict)


        out_event = {}

        schema = self.schema()
        for f in left:
            if f in self._same_fields:
                new_f = self._prefix_duplicate_field(field=f, is_left=True)
                out_event[new_f] = left[f]
            else:
                # skip fields not in schema
                if f not in schema.fields():
                    continue
                out_event[f] = left[f]

        for f in right:
            if f in self._same_fields:
                new_f = self._prefix_duplicate_field(field=f, is_left=False)
                out_event[new_f] = right[f]
            else:
                # skip fields not in schema
                if f not in schema.fields():
                    continue
                out_event[f] = right[f]

        return out_event


class Rename(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: Dict[str, str]):
        super().__init__()
        self.column_mapping = columns
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        raise NotImplementedError()


# TODO Drop is used for both drop() and select() API, indicate difference?
class Drop(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: List[str]):
        super().__init__()
        self.parents.append(parent)
        self.columns = columns

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
