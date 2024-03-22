import inspect
from dataclasses import dataclass
from typing import Callable, Type, Optional, List, cast, TypeVar, Union, Dict

from decimal import Decimal

from volga.common.time_utils import datetime_str_to_ts
from volga.data.api.consts import RESERVED_FIELD_NAMES, PIPELINE_ATTR, CONNECTORS_ATTR
from volga.data.api.dataset.operator import OperatorNode
from volga.data.api.dataset.schema import DatasetSchema

import datetime

from volga.data.api.source.source import Connector
from volga.data.api.utils import is_optional
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.message.message import Record
from volga.streaming.api.operator.timestamp_assigner import EventTimeAssigner

T = TypeVar('T')


# decorator to construct Dataset from user defined class
def dataset(
    cls: Optional[Type[T]] = None,
) -> Union[Callable, 'Dataset']:

    def _create_dataset(
        dataset_cls: Type[T],
    ) -> Dataset:
        cls_annotations = dataset_cls.__dict__.get("__annotations__", {})
        fields = [
            get_field(
                cls=dataset_cls,
                annotation_name=name,
                dtype=cls_annotations[name],
            )
            for name in cls_annotations
        ]

        return Dataset(
            dataset_cls,
            fields,
        )

    def wrap(c: Type[T]) -> Dataset:
        return _create_dataset(c)

    if cls is None:
        # called as @dataset(arguments)
        return wrap
    cls = cast(Type[T], cls)
    # @dataset decorator was used without arguments
    return wrap(cls)


@dataclass
class Field:
    name: Optional[str]
    dataset_name: Optional[str]
    dataset: Optional['Dataset']
    key: bool
    timestamp: bool
    dtype: Optional[Type]

    def __str__(self):
        return f'{self.name}'

    def is_optional(self) -> bool:
        return is_optional(self.dtype)


def get_field(
    cls: T,
    annotation_name: str,
    dtype: Type,
) -> Field:
    if '.' in annotation_name:
        raise ValueError(
            f'Field name {annotation_name} cannot contain a period.'
        )
    field = getattr(cls, annotation_name, None)
    if isinstance(field, Field):
        field.name = annotation_name
        field.dtype = dtype
        field.dataset_name = cls.__name__  # type: ignore
    else:
        field = Field(
            name=annotation_name,
            dataset_name=cls.__name__,  # type: ignore
            dataset=None,  # set as part of dataset initialization
            key=False,
            timestamp=False,
            dtype=dtype,
        )

    if field.key and field.is_optional():
        raise ValueError(
            f"Key {annotation_name} in dataset {cls.__name__} cannot be "  # type: ignore
            f"Optional."
        )
    return field


class Dataset(OperatorNode):
    _name: str
    _fields: List[Field]
    _key_fields: List[str]
    _pipeline: Optional['Pipeline']
    _timestamp_field: str

    def __init__(
        self,
        cls: T,
        fields: List[Field],
    ):
        super().__init__()
        self._name = cls.__name__  # type: ignore
        self.__name__ = self._name
        self._fields = fields
        self._validate_field_names(fields)
        self._original_cls = cls
        self._add_fields_to_class()
        self._set_timestamp_field()
        self._set_key_fields()
        self._pipeline = self.get_pipeline()

    def __repr__(self):
        return self._name

    def dataset_schema(self):
        return DatasetSchema(
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

        # Find a field that has datetime type and set it as timestamp
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

    def get_pipeline(self) -> Optional['Pipeline']:
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

    def _get_source_connectors(self) -> Optional[Dict[str, Connector]]:
        return getattr(self, CONNECTORS_ATTR, None)

    def is_source(self) -> bool:
        return hasattr(self, CONNECTORS_ATTR)

    def init_stream(self, source_tag: Optional[str], ctx: StreamingContext):
        if not self.is_source():
            raise ValueError(f'Dataset {self._name}: Can not get source stream from non-source dataset')
        if self.stream is not None:
            raise ValueError(f'Dataset {self._name}: Stream source already inited')

        source_connectors = self._get_source_connectors()
        assert source_connectors is not None
        if source_tag is None:
            if len(source_connectors) > 1:
                raise ValueError(f'Dataset {self._name}: Need to specify tag for source with > 1 connectors')
            connector = list(source_connectors.values())[0]
        else:
            if source_tag not in source_connectors:
                raise ValueError(f'Dataset {self._name}: Can not find source tag {source_tag}')
            connector = source_connectors[source_tag]

        stream_source = connector.to_stream_source(ctx)

        # set timestamp assigner
        if self._timestamp_field is None:
            raise RuntimeError('Can not init source with no timestamp field')

        def _extract_timestamp(record: Record) -> Decimal:
            dt_str = record.value[self._timestamp_field]
            return datetime_str_to_ts(dt_str)

        stream_source.timestamp_assigner(EventTimeAssigner(_extract_timestamp))

        self.stream = stream_source


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

