
from dataclasses import dataclass
from typing import Type, Optional, List, cast, TypeVar, Dict

from volga.api.consts import RESERVED_FIELD_NAMES
from volga.api.operators import OperatorNode
from volga.api.schema import Schema

import datetime

# from volga.api.utils import is_optional

T = TypeVar('T')

@dataclass
class Field:
    name: Optional[str]
    dataset_name: Optional[str]
    dataset: Optional['Entity']
    key: bool
    timestamp: bool
    dtype: Optional[Type]

    def __str__(self):
        return f'{self.name}'


class EntityMetadata:
    """
    Stores metadata about an entity class.
    This is a stateless class that defines the structure and properties of an entity.
    """
    def __init__(
        self,
        cls: Type,
        fields: List[Field],
    ):
        self.name = cls.__name__
        self.fields = fields
        self.original_cls = cls
        self._validate_field_names(fields)
        self._set_timestamp_field()
        self._set_key_fields()
        
        # Initialize feature registries
        self._pipelines: Dict[str, 'PipelineFeature'] = {}
        self._on_demands: Dict[str, 'OnDemandFeature'] = {}

    def __repr__(self):
        return self.name

    def schema(self) -> Schema:
        return Schema(
            keys={f.name: f.dtype for f in self.fields if f.key},
            values={
                f.name: f.dtype
                for f in self.fields
                if not f.key and f.name != self.timestamp_field
            },
            timestamp=self.timestamp_field,
        )

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self.fields:
            if field.timestamp:
                self.timestamp_field = field.name
                if timestamp_field_set:
                    raise ValueError('multiple timestamp fields are not supported')
                timestamp_field_set = True

        if timestamp_field_set:
            return

        # Find a field that has datetime type and set it as timestamp
        for field in self.fields:
            if field.dtype != datetime.datetime and field.dtype != "datetime":
                continue
            if not timestamp_field_set:
                field.timestamp = True
                timestamp_field_set = True
                self.timestamp_field = field.name
            else:
                raise ValueError('multiple timestamp fields are not supported')
        if not timestamp_field_set:
            raise ValueError('no timestamp field found')

    def _set_key_fields(self):
        key_fields = []
        for field in self.fields:
            if field.key:
                key_fields.append(field.name)
        self.key_fields = key_fields

    def _validate_field_names(self, fields: List[Field]):
        names = set()
        exceptions = []
        for f in fields:
            if f.name in names:
                raise Exception(f'Duplicate field name {f.name} found in dataset {self.name}')
            names.add(f.name)
            if f.name in RESERVED_FIELD_NAMES:
                exceptions.append(
                    Exception(f'Field name {f.name} is reserved, please use a different name in dataset {self.name}')
                )
        if len(exceptions) != 0:
            raise Exception(exceptions)

    def register_pipeline_feature(self, name: str, feature: 'PipelineFeature') -> None:
        """Register a pipeline feature"""
        self._pipelines[name] = feature

    def register_on_demand_feature(self, name: str, feature: 'OnDemandFeature') -> None:
        """Register an on-demand feature"""
        self._on_demands[name] = feature

    def get_pipeline_features(self) -> Dict[str, 'PipelineFeature']:
        """Get all pipeline features"""
        return self._pipelines

    def get_on_demand_features(self) -> Dict[str, 'OnDemandFeature']:
        """Get all on-demand features"""
        return self._on_demands


class Entity(OperatorNode):
    """
    Represents an entity instance in a pipeline.
    Each pipeline should have its own Entity instances.
    """
    def __init__(self, entity_cls: Type):
        super().__init__()
        validate_decorated_entity(entity_cls, "entity_cls", "Entity constructor")
        self.metadata = entity_cls._entity_metadata
        self.entity_cls = entity_cls
        
    def schema(self) -> Schema:
        return self.metadata.schema()


def entity(*args, **kwargs):
    def wrap(cls):
        # Check for any methods defined in the class
        for name, value in cls.__dict__.items():
            if callable(value) and not name.startswith('__'):
                raise TypeError(
                    f"Entity '{cls.__name__}' cannot have methods. "
                    f"Found method: '{name}'. Entities should only contain data fields."
                )

        # Get class annotations (type hints)
        cls_annotations = cls.__annotations__ if hasattr(cls, '__annotations__') else {}
        
        # Collect fields
        fields = []
        for name, dtype in cls_annotations.items():
            field_value = getattr(cls, name, None)
            
            if isinstance(field_value, Field):
                # Update field with name and type
                field_value.name = name
                field_value.dtype = dtype
                field_value.dataset_name = cls.__name__
                fields.append(field_value)
            else:
                # Create default field if not specified
                field = Field(
                    name=name,
                    dataset_name=cls.__name__,
                    dataset=None,
                    key=False,
                    timestamp=False,
                    dtype=dtype,
                )
                fields.append(field)

        def __init__(self, **kwargs):
            # Set default values first
            for field in fields:
                if not field.name:
                    continue
                # Get default value if exists
                default_val = getattr(cls, field.name, None)
                if isinstance(default_val, Field):
                    default_val = None
                setattr(self, field.name, default_val)

            # Override with provided values
            for key, value in kwargs.items():
                if key not in cls_annotations:
                    raise TypeError(f"{cls.__name__}.__init__ got unexpected argument '{key}'")
                setattr(self, key, value)

            # Check for required fields (those without defaults)
            missing = []
            for field in fields:
                if not field.name:
                    continue
                if not hasattr(self, field.name) or getattr(self, field.name) is None:
                    default_val = getattr(cls, field.name, None)
                    if isinstance(default_val, Field):
                        missing.append(field.name)
            
            if missing:
                raise TypeError(f"{cls.__name__}.__init__ missing required arguments: {', '.join(missing)}")

        # Set the new init method
        cls.__init__ = __init__

        # Create and store EntityMetadata
        metadata = EntityMetadata(cls, fields)
        cls._entity_metadata = metadata

        return cls

    # Handle both @entity and @entity() syntax
    if len(args) == 1 and not kwargs and callable(args[0]):
        return wrap(args[0])
    return wrap


def create_entity(entity_cls: Type) -> Entity:
    """
    Create a new Entity instance for a given entity class.
    This should be called for each pipeline that uses the entity.
    """
    return Entity(entity_cls)

def validate_decorated_entity(type_annotation: Type, param_name: str, feature_name: str) -> None:
    """Validate that a type is decorated with @entity"""
    if not hasattr(type_annotation, '_entity_metadata'):
        raise TypeError(
            f'{param_name} type of {feature_name} must be decorated with @entity. '
        )
    
def is_entity_type(type_hint):
    if hasattr(type_hint, '_entity_metadata'):
        return True
    # Check if it's a List of entities
    origin = getattr(type_hint, '__origin__', None)
    if origin is list or origin is List:
        args = getattr(type_hint, '__args__', [])
        return any(hasattr(arg, '_entity_metadata') for arg in args)
    return False
    
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

    # def init_stream(self, source_tag: Optional[str], ctx: StreamingContext):
    #     if not self.is_source():
    #         raise ValueError(f'Dataset {self._name}: Can not get source stream from non-source dataset')
    #     if self.stream is not None:
    #         raise ValueError(f'Dataset {self._name}: Stream source already inited')

    #     source_connectors = self._get_source_connectors()
    #     assert source_connectors is not None
    #     if source_tag is None:
    #         if len(source_connectors) > 1:
    #             raise ValueError(f'Dataset {self._name}: Need to specify tag for source with > 1 connectors')
    #         connector = list(source_connectors.values())[0]
    #     else:
    #         if source_tag not in source_connectors:
    #             raise ValueError(f'Dataset {self._name}: Can not find source tag {source_tag}')
    #         connector = source_connectors[source_tag]

    #     stream_source = connector.to_stream_source(ctx)

    #     # set timestamp assigner
    #     if self._timestamp_field is None:
    #         raise RuntimeError('Can not init source with no timestamp field')

    #     def _extract_timestamp(record: Record) -> Decimal:
    #         dt_str = record.value[self._timestamp_field]
    #         return datetime_str_to_ts(dt_str)

    #     stream_source.timestamp_assigner(EventTimeAssigner(_extract_timestamp))

    #     self.stream = stream_source

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
