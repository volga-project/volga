import copy
import inspect
from dataclasses import dataclass
from typing import Callable, Dict, Type, Optional, List, cast, TypeVar

from volga.data.api.consts import RESERVED_FIELD_NAMES, PIPELINE_ATTR
from volga.data.api.dataset.pipeline import Pipeline
from volga.data.api.dataset.schema import DataSetSchema
from volga.data.api.operator.operator import Node, Transform, Filter, Assign, GroupBy, Join, Rename, Drop, DropNull

import datetime

T = TypeVar("T")

# decorator to construct Dataset from user defined class
def dataset():
    pass

@dataclass
class Field:
    name: Optional[str]
    dataset_name: Optional[str]
    dataset: Optional['Dataset']
    key: bool
    timestamp: bool
    dtype: Optional[Type]

    def __str__(self):
        return f"{self.name}"


class Dataset(Node):
    _name: str
    _fields: List[Field]
    _key_fields: List[str]
    _pipeline: Optional[Pipeline]
    _timestamp_field: str
    is_terminal: bool

    def __init__(
        self,
        cls: T,
        fields: List[Field],
    ):
        super().__init__()
        self._name = cls.__name__  # type: ignore
        self.__name__ = self._name
        self.is_terminal = False
        self._fields = fields
        self._validate_field_names(fields)
        self._original_cls = cls
        self._add_fields_to_class()
        self._set_timestamp_field()
        self._set_key_fields()
        self._pipeline = self._get_pipeline()

    def data_set_schema(self):
        return DataSetSchema(
            keys={f.name: f.dtype for f in self._fields if f.key},
            values={
                f.name: f.dtype
                for f in self._fields
                if not f.key and f.name != self._timestamp_field
            },
            timestamp=self._timestamp_field,
            name=f"'[Dataset:{self._name}]'",
        )

    def _add_fields_to_class(self) -> None:
        for field in self._fields:
            if not field.name:
                continue
            setattr(self, field.name, field)

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self._fields:
            if field.timestamp:
                self._timestamp_field = field.name
                if timestamp_field_set:
                    raise ValueError('multiple timestamp fields are not supported')
                timestamp_field_set = True

        if timestamp_field_set:
            return

        # Find a field that has datetime type and set it as timestamp.

        for field in self._fields:
            if field.dtype != datetime.datetime and field.dtype != "datetime":
                continue
            if not timestamp_field_set:
                field.timestamp = True
                timestamp_field_set = True
                self._timestamp_field = field.name
            else:
                raise ValueError('multiple timestamp fields are not supported')
        if not timestamp_field_set:
            raise ValueError('no timestamp field found')

    def _set_key_fields(self):
        key_fields = []
        for field in self._fields:
            if field.key:
                key_fields.append(field.name)
        self._key_fields = key_fields

    def _get_pipeline(self) -> Optional[Pipeline]:
        for name, method in inspect.getmembers(self._original_cls):
            if not callable(method):
                continue
            if not hasattr(method, PIPELINE_ATTR):
                continue

            pipeline = getattr(method, PIPELINE_ATTR)

            if pipeline is not None:
                return pipeline

        return None

    def _validate_field_names(self, fields: List[Field]):
        names = set()
        exceptions = []
        for f in fields:
            if f.name in names:
                raise Exception(f'Duplicate field name {f.name} found in dataset {self._name}')
            names.add(f.name)
            if f.name in RESERVED_FIELD_NAMES:
                exceptions.append(
                    Exception(f'Field name {f.name} is reserved, please use a different name in dataset {self._name}')
                )
        if len(exceptions) != 0:
            raise Exception(exceptions)


def field(
    key: bool = False,
    timestamp: bool = False,
) -> T:
    return cast(
        T,
        Field(
            key=key,
            dataset_name=None,
            dataset=None,
            timestamp=timestamp,
            name=None,
            dtype=None,
        ),
    )


# user facing operators to construct pipeline graph
class Node:

    def __init__(self):
        self.out_edges = []

    def data_set_schema(self) -> DataSetSchema:
        raise NotImplementedError()

    def transform(self, func: Callable, schema: Dict = {}) -> Node:
        if schema == {}:
            return Transform(self, func, None)
        return Transform(self, func, copy.deepcopy(schema))

    def filter(self, func: Callable) -> Node:
        return Filter(self, func)

    def assign(self, column: str, result_type: Type, func: Callable) -> Node:
        return Assign(self, column, result_type, func)

    def groupby(self, *args) -> GroupBy:
        return GroupBy(self, *args)

    def join(
        self,
        other: Dataset,
        on: List[str],
    ) -> Join:
        if not isinstance(other, Dataset) and isinstance(other, Node):
            raise ValueError(
                "Cannot join with an intermediate dataset, i.e something defined inside a pipeline."
                " Only joining against keyed datasets is permitted."
            )
        if not isinstance(other, Node):
            raise TypeError("Cannot join with a non-dataset object")
        return Join(self, other, on)

    def rename(self, columns: Dict[str, str]) -> Node:
        return Rename(self, columns)

    def drop(self, columns: List[str]) -> Node:
        return Drop(self, columns, name="drop")

    def dropnull(self, columns: List[str]) -> 'Node':
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

