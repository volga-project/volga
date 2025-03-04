from datetime import datetime
import functools
from typing import Callable, Dict, Type, List, Optional, Any

from volga.common.time_utils import is_time_str
from volga.api.aggregate import AggregateType, Avg, Count, Max, Min, Sum
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

    def transform(self, func: Callable, new_schema_dict: Optional[Dict[str, Type]] = None) -> 'OperatorNode':
        return Transform(self, func, new_schema_dict)

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
        self.output_schema = self.schema()

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def _stream_map_func(self, event: Any) -> Any:
        # Validate input matches parent schema
        parent_schema = self.parents[0].schema()
        input_fields = set(event.keys())
        expected_fields = set(parent_schema.fields())
        if input_fields != expected_fields:
            raise ValueError(f'Input event fields {input_fields} do not match parent schema fields {expected_fields}')

        # Apply transform
        result = self.func(event)

        # Validate output matches target schema
        output_fields = set(result.keys())
        expected_fields = set(self.output_schema.fields())
        if output_fields != expected_fields:
            raise ValueError(f'Output event fields {output_fields} do not match target schema fields {expected_fields}')

        return result

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
        self.column = column
        self.output_type = output_type
        self.func = func

    def init_stream(self):
        self.stream = self.parents[0].stream.map(map_func=self._stream_map_func)

    def schema(self) -> Schema:
        parent_schema = self.parents[0].schema()
        ts = parent_schema.timestamp

        # Cannot assign to timestamp field
        if self.column == ts:
            raise ValueError(f'Cannot assign to timestamp field {ts}')

        # Cannot assign to key fields
        if self.column in parent_schema.keys:
            raise ValueError(f'Cannot assign to key field {self.column}')

        # Create new schema with added/updated column
        keys = parent_schema.keys.copy()
        values = parent_schema.values.copy()
        values[self.column] = self.output_type

        return Schema(
            keys=keys,
            values=values,
            timestamp=ts
        )

    def _stream_map_func(self, event: Any) -> Any:
        # Validate input matches parent schema
        parent_schema = self.parents[0].schema()
        input_fields = set(event.keys())
        expected_fields = set(parent_schema.fields())
        if input_fields != expected_fields:
            raise ValueError(f'Input event fields {input_fields} do not match parent schema fields {expected_fields}')

        # Create new dict with assigned column
        result = event.copy()
        result[self.column] = self.func(event)

        # Validate output matches target schema
        output_schema = self.schema()
        output_fields = set(result.keys())
        expected_fields = set(output_schema.fields())
        if output_fields != expected_fields:
            raise ValueError(f'Output event fields {output_fields} do not match target schema fields {expected_fields}')

        return result


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
        return self.parents[0].schema().copy()


class Aggregate(OperatorNode):
    def __init__(
        self, parent: OperatorNodeBase, aggregates: List[AggregateType]
    ):
        super().__init__()
        self.aggregates = aggregates
        self.parents.append(parent)
        self.keys = parent.keys

    def schema(self) -> Schema:
        input_schema = self.parents[0].schema()

        keys = {f: input_schema.get_type(f) for f in self.keys}
        values = {}
        for agg in self.aggregates:
            if isinstance(agg, Count):
                values[agg.into] = int
            elif isinstance(agg, Sum):
                values[agg.into] = float
            elif isinstance(agg, Min):
                values[agg.into] = float
            elif isinstance(agg, Max):
                values[agg.into] = float
            elif isinstance(agg, Avg):
                values[agg.into] = float
            else:
                raise ValueError(f'Unsupported aggregate type: {type(agg)}')
        
        return Schema(
            keys=keys,
            values=values,
            timestamp=input_schema.timestamp,
        )

    def init_stream(self):
        output_schema = self.schema()

        def _output_window_func(aggs_per_window: AggregationsPerWindow, record: Record) -> Record:
            record_value = record.value
            res = {}

            # copy keys
            expected_key_fields = list(output_schema.keys.keys())
            for k in expected_key_fields:
                if k not in record_value:
                    raise RuntimeError(f'Can not locate key field {k}')
                res[k] = record_value[k]

            ts_field = output_schema.timestamp

            if ts_field not in record_value:
                raise RuntimeError(f'Unable to locate timestamp field {ts_field} in record value {record_value}')
            res[ts_field] = record_value[ts_field]

            # copy aggregate values
            values_fields = list(output_schema.values.keys())
            for v in values_fields:
                if v not in aggs_per_window:
                    raise RuntimeError(f'Unable to locate {v} in aggregates: {aggs_per_window}')
                res[v] = aggs_per_window[v]

            return Record.new_value(value=res, record=record)

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
        self.keys = sorted(keys)  # Sort keys for consistent behavior
        self.parents.append(parent)

    def init_stream(self):
        self.stream = self.parents[0].stream.key_by(key_by_func=self._stream_key_by_func)

    def _stream_key_by_func(self, event: Any) -> Any:
        assert isinstance(event, Dict)
        if len(self.keys) == 1:
            key = self.keys[0]
            if key not in event:
                raise RuntimeError(f'key {key} not in event {event}')
            return event[key]
        
        # For multiple keys, use frozenset for order independence
        return frozenset((k, event[k]) for k in self.keys)

    def aggregate(self, aggregates: List[AggregateType]) -> OperatorNode:
        if len(aggregates) == 0:
            raise ValueError('Aggregate expects at least one aggregation operation')
        return Aggregate(self, aggregates)

    def schema(self) -> Schema:
        # group by does not alter parent's schema
        return self.parents[0].schema().copy()


class Join(OperatorNode):
    def __init__(
        self,
        left: OperatorNode,
        right: OperatorNode,
        how: str = 'left',
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
        left_prefix: str = 'left',
        right_prefix: str = 'right',
    ):
        super().__init__()
        self.left = left
        self.right = right

        self.parents.append(left)
        self.parents.append(right)
        self.on = sorted(on) if on is not None else None

        self.how = how
        self.left_prefix = left_prefix
        self.right_prefix = right_prefix

        if left_on is None and right_on is not None or \
                left_on is not None and right_on is None:
            raise ValueError('Join expects both left_on and right_on')

        self.left_on = sorted(left_on) if left_on is not None else None
        self.right_on = sorted(right_on) if right_on is not None else None

        # fields with the same name
        self._same_fields = list(set(self.left.schema().fields()) & set(self.right.schema().fields()))

    def init_stream(self):
        self.stream = self.left.stream.key_by(self._stream_left_key_func) \
            .join(self.right.stream.key_by(self._stream_right_key_func)) \
            .with_func(self._stream_join_func)

    @staticmethod
    def _prefix_duplicate_field(field: str, is_left: bool, left_prefix: str, right_prefix: str):
        if is_left:
            return f'{left_prefix}_{field}'
        else:
            return f'{right_prefix}_{field}'

    @staticmethod
    def _joined_schema(ls: Schema, rs: Schema, how: str, on: Optional[List[str]], left_on: Optional[List[str]], right_on: Optional[List[str]], left_prefix: str = 'left', right_prefix: str = 'right'):
        same_fields = list(set(ls.fields()) & set(rs.fields()))
        
        # Determine which schema to use as primary for timestamp and keys
        primary_schema = ls if how == 'left' else rs
        secondary_schema = rs if how == 'left' else ls
        ts = primary_schema.timestamp

        keys = {}
        values = {}
        
        # Handle join keys
        if on is not None:
            for k in on:
                keys[k] = primary_schema.keys[k]
        else:
            primary_keys = left_on if how == 'left' else right_on
            for k in primary_keys:
                keys[k] = primary_schema.keys[k]

        # Process fields from primary schema
        for f in primary_schema.fields():
            if f == ts or f in keys:
                continue
            renamed_f = f
            if f in same_fields:  # Only prefix if field exists in both schemas
                renamed_f = Join._prefix_duplicate_field(
                    field=f,
                    is_left=(how == 'left'),
                    left_prefix=left_prefix,
                    right_prefix=right_prefix
                )
            if renamed_f in values:
                raise RuntimeError(f'Duplicate entry for field {renamed_f}')
            if f in primary_schema.values:
                values[renamed_f] = primary_schema.values[f]
            elif f in primary_schema.keys and f not in keys:
                values[renamed_f] = primary_schema.keys[f]

        # Process fields from secondary schema
        for f in secondary_schema.fields():
            if f == ts or f in keys:
                continue
            renamed_f = f
            if f in same_fields:  # Only prefix if field exists in both schemas
                renamed_f = Join._prefix_duplicate_field(
                    field=f,
                    is_left=(how != 'left'),
                    left_prefix=left_prefix,
                    right_prefix=right_prefix
                )
            if renamed_f in values:
                raise RuntimeError(f'Duplicate entry for field {renamed_f}')
            if f in secondary_schema.values:
                values[renamed_f] = secondary_schema.values[f]
            elif f in secondary_schema.keys and f not in keys:
                values[renamed_f] = secondary_schema.keys[f]
            elif f == secondary_schema.timestamp:
                values[f] = datetime

        return Schema(
            keys=keys,
            values=values,
            timestamp=ts
        )

    def schema(self) -> Schema:
        return Join._joined_schema(self.left.schema(), self.right.schema(), self.how, self.on, self.left_on, self.right_on, self.left_prefix, self.right_prefix)

    def _stream_left_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        keys = self.left_on if self.on is None else self.on
        if len(keys) == 1:
            return element[keys[0]]
        # Keys are already sorted during initialization
        return frozenset((k, element[k]) for k in keys)

    def _stream_right_key_func(self, element: Any) -> Any:
        assert isinstance(element, Dict)
        keys = self.right_on if self.on is None else self.on
        if len(keys) == 1:
            return element[keys[0]]
        # Keys are already sorted during initialization
        return frozenset((k, element[k]) for k in keys)

    def _stream_join_func(self, left: Any, right: Any) -> Any:
        if left is None or right is None:
            raise RuntimeError('Can not join null values')
        assert isinstance(left, Dict)
        assert isinstance(right, Dict)

        out_event = {}
        schema = self.schema()

        # Copy key fields from primary side
        for k in schema.keys:
            if self.how == 'left':
                out_event[k] = left[k]
            else:
                out_event[k] = right[k]

        # Copy timestamp from primary side
        ts_field = schema.timestamp
        if self.how == 'left':
            out_event[ts_field] = left[ts_field]
        else:
            out_event[ts_field] = right[ts_field]

        # Process fields from left side
        for f in left:
            if f == ts_field or f in schema.keys:
                continue
            new_f = f
            if f in self._same_fields:
                new_f = Join._prefix_duplicate_field(
                    field=f,
                    is_left=True,
                    left_prefix=self.left_prefix,
                    right_prefix=self.right_prefix
                )
            if new_f in schema.values:  # Only copy if field is in target schema
                out_event[new_f] = left[f]

        # Process fields from right side
        for f in right:
            if f == ts_field or f in schema.keys:
                continue
            new_f = f
            if f in self._same_fields:
                new_f = Join._prefix_duplicate_field(
                    field=f,
                    is_left=False,
                    left_prefix=self.left_prefix,
                    right_prefix=self.right_prefix
                )
            if new_f in schema.values:  # Only copy if field is in target schema
                out_event[new_f] = right[f]

        return out_event


class Rename(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: Dict[str, str]):
        super().__init__()
        self.column_mapping = columns
        self.parents.append(parent)

    def schema(self) -> Schema:
        parent_schema = self.parents[0].schema()
        keys = {}
        values = {}
        ts = parent_schema.timestamp

        # Handle key fields
        for old_name, type_ in parent_schema.keys.items():
            new_name = self.column_mapping.get(old_name, old_name)
            if new_name == ts:
                raise ValueError(f'Cannot rename field {old_name} to timestamp field name {ts}')
            keys[new_name] = type_

        # Handle value fields
        for old_name, type_ in parent_schema.values.items():
            new_name = self.column_mapping.get(old_name, old_name)
            if new_name == ts:
                raise ValueError(f'Cannot rename field {old_name} to timestamp field name {ts}')
            values[new_name] = type_

        return Schema(
            keys=keys,
            values=values,
            timestamp=ts
        )

    def _stream_map_func(self, event: Any) -> Any:
        # Validate input matches parent schema
        parent_schema = self.parents[0].schema()
        input_fields = set(event.keys())
        expected_fields = set(parent_schema.fields())
        if input_fields != expected_fields:
            raise ValueError(f'Input event fields {input_fields} do not match parent schema fields {expected_fields}')

        # Apply rename
        result = {}
        for old_name, value in event.items():
            new_name = self.column_mapping.get(old_name, old_name)
            result[new_name] = value

        # Validate output matches target schema
        output_schema = self.schema()
        output_fields = set(result.keys())
        expected_fields = set(output_schema.fields())
        if output_fields != expected_fields:
            raise ValueError(f'Output event fields {output_fields} do not match target schema fields {expected_fields}')

        return result


# TODO Drop is used for both drop() and select() API, indicate difference?
class Drop(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: List[str]):
        super().__init__()
        self.parents.append(parent)
        # Only keep columns that exist in parent schema
        parent_schema = parent.schema()
        self.columns = [col for col in columns if col in parent_schema.fields()]

    def schema(self) -> Schema:
        parent_schema = self.parents[0].schema()
        ts = parent_schema.timestamp

        # Cannot drop timestamp field
        if ts in self.columns:
            raise ValueError(f'Cannot drop timestamp field {ts}')

        # Cannot drop key fields
        key_fields = set(parent_schema.keys.keys())
        drop_fields = set(self.columns)
        if key_fields & drop_fields:
            invalid_keys = key_fields & drop_fields
            raise ValueError(f'Cannot drop key fields: {invalid_keys}')

        # Create new schema without dropped fields
        keys = parent_schema.keys
        values = {
            field: type_
            for field, type_ in parent_schema.values.items()
            if field not in drop_fields
        }

        return Schema(
            keys=keys,
            values=values,
            timestamp=ts
        )

    def _stream_map_func(self, event: Any) -> Any:
        # Validate input matches parent schema
        parent_schema = self.parents[0].schema()
        input_fields = set(event.keys())
        expected_fields = set(parent_schema.fields())
        if input_fields != expected_fields:
            raise ValueError(f'Input event fields {input_fields} do not match parent schema fields {expected_fields}')

        # Create new dict without dropped fields
        result = {
            field: value
            for field, value in event.items()
            if field not in self.columns
        }

        # Validate output matches target schema
        output_schema = self.schema()
        output_fields = set(result.keys())
        expected_fields = set(output_schema.fields())
        if output_fields != expected_fields:
            raise ValueError(f'Output event fields {output_fields} do not match target schema fields {expected_fields}')

        return result


class DropNull(OperatorNode):
    def __init__(self, parent: OperatorNodeBase, columns: Optional[List[str]] = None):
        super().__init__()
        self.parents.append(parent)
        # If columns is None, check all fields except keys and timestamp
        if columns is None:
            parent_schema = parent.schema()
            self.columns = list(parent_schema.values.keys())
        else:
            # Only keep columns that exist in parent schema values
            parent_schema = parent.schema()
            self.columns = [
                col for col in columns 
                if col in parent_schema.values
            ]

    def init_stream(self):
        self.stream = self.parents[0].stream.filter(filter_func=self._stream_filter_func)

    def schema(self) -> Schema:
        return self.parents[0].schema().copy()

    def _stream_filter_func(self, event: Any) -> bool:
        # Validate input matches parent schema
        parent_schema = self.parents[0].schema()
        input_fields = set(event.keys())
        expected_fields = set(parent_schema.fields())
        if input_fields != expected_fields:
            raise ValueError(f'Input event fields {input_fields} do not match parent schema fields {expected_fields}')

        # Keep event only if none of the specified columns have None value
        return not any(event[col] is None for col in self.columns)
