from abc import ABC, abstractmethod
import datetime
import decimal
import msgpack
from typing import Any, Dict, Type


class Serializer(ABC):
    """
    Abstract base class for serializers used in network communication.
    """
    
    @abstractmethod
    def dumps(self, obj: Any) -> bytes:
        """Serialize obj to bytes."""
        pass
    
    @abstractmethod
    def loads(self, data: bytes) -> Any:
        """Deserialize bytes to Python objects."""
        pass


class MsgpackSerializer(Serializer):
    """
    Serializer implementation using msgpack that handles special types like datetime.
    """
    
    @staticmethod
    def default_serializer(obj: Any) -> Any:
        """Convert Python objects to msgpack-serializable types."""
        if isinstance(obj, datetime.datetime):
            return {"__datetime__": obj.isoformat()}
        elif isinstance(obj, decimal.Decimal):
            return {"__decimal__": str(obj)}
        elif isinstance(obj, set) or isinstance(obj, frozenset):
            return {"__set__": list(obj)}
        raise TypeError(f"Object of type {type(obj)} is not serializable")
    
    @staticmethod
    def object_hook(obj: Dict) -> Any:
        """Convert msgpack-serialized objects back to Python types."""
        if "__datetime__" in obj and len(obj) == 1:
            return datetime.datetime.fromisoformat(obj["__datetime__"])
        elif "__decimal__" in obj and len(obj) == 1:
            return decimal.Decimal(obj["__decimal__"])
        elif "__set__" in obj and len(obj) == 1:
            return set(obj["__set__"])
        return obj
    
    def dumps(self, obj: Any) -> bytes:
        """Serialize obj to msgpack format."""
        return msgpack.dumps(obj, default=self.default_serializer)
    
    def loads(self, data: bytes) -> Any:
        """Deserialize msgpack data to Python objects."""
        return msgpack.loads(data, object_hook=self.object_hook)


# Default serializer instance to use
DEFAULT_SERIALIZER = MsgpackSerializer() 