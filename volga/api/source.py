from typing import TypeVar, List, Any, Callable, Type
import inspect
from functools import wraps

from pydantic import BaseModel

from volga.api.entity import validate_decorated_entity
from volga.api.pipeline import create_and_register_pipeline_feature
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream_source import StreamSource

T = TypeVar('T')


# describes single connection
class Connector:

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        raise NotImplementedError()


class KafkaConnector(Connector):

    def __init__(self, source, topic):
        self.source = source
        self.topic = topic

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        return ctx.kafka(
            bootstrap_servers=self.source.bootstrap_servers,
            topic=self.topic,
            sasl_plain_username=self.source.username,
            sasl_plain_password=self.source.password
        )


class MysqlConnector(Connector):

    def __init__(self, source, table):
        self.source = source
        self.table = table

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        return ctx.mysql(
            host=self.source.host,
            port=self.source.port,
            user=self.source.user,
            password=self.source.password,
            database=self.source.database,
            table=self.table
        )


class MockOfflineConnector(Connector):

    def __init__(self, items: List[Any]):
        self.items = items

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        return ctx.from_collection(self.items)
    
    @staticmethod
    def with_items(items: List[Any]) -> 'MockOfflineConnector':
        return MockOfflineConnector(items)


class MockOnlineConnector(Connector):

    def __init__(self, items: List[Any], period_s: int):
        self.items = items
        self.period_s = period_s

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        return ctx.from_periodic_collection(self.items, period_s=self.period_s)
    
    @staticmethod
    def with_periodic_items(items: List[Any], period_s: int) -> 'MockOnlineConnector':
        return MockOnlineConnector(items, period_s=period_s)


# produces connections
class Source(BaseModel):

    @staticmethod
    def get(*args, **kwargs) -> 'Source':
        raise NotImplementedError()


def source(output: Type) -> Callable:
    # Validate output type has @entity decorator
    validate_decorated_entity(output, 'Output', 'source decorator')  
    
    def wrapper(source_func: Callable) -> Callable:
        if not callable(source_func):
            raise TypeError('source functions must be callable')
        
        feature_name = source_func.__name__
        
        # Get function signature and parameters
        sig = inspect.signature(source_func)
        params = list(sig.parameters.values())
        
        # Source functions should have no required parameters without defaults
        required_params = [p for p in params if p.default == inspect.Parameter.empty]
        if required_params:
            raise TypeError(
                f'Source function {feature_name} should not have required parameters, '
                f'got {len(required_params)} required parameters'
            )
        
        # Validate return type is a Connector
        return_type = sig.return_annotation
        if return_type == inspect.Parameter.empty:
            raise TypeError(
                f'Source function {feature_name} must have a return type annotation'
            )
        
        if not issubclass(return_type, Connector):
            raise TypeError(
                f'Source function {feature_name} must return a Connector instance, '
                f'got {return_type} instead'
            )
        
        # Create and register pipeline feature with empty dependencies
        create_and_register_pipeline_feature(
            func=source_func,
            feature_name=feature_name,
            dep_args=[],  # Sources have no dependencies
            output_type=output,
            is_source=True
        )
        
        @wraps(source_func)
        def wrapped_func(*args, **kwargs):
            # Call the original function
            result = source_func(*args, **kwargs)
            
            # Validate return value is a Connector instance
            if not isinstance(result, Connector):
                raise TypeError(
                    f'Return value of {feature_name} must be an instance of Connector, '
                    f'got {type(result).__name__} instead'
                )
            
            return result
        
        return wrapped_func
    
    return wrapper


class KafkaSource(Source):
    bootstrap_servers: str
    username: str
    password: str

    @staticmethod
    def get(
        bootstrap_servers: str,
        username: str,
        password: str
    ) -> 'KafkaSource':
        return KafkaSource(
            bootstrap_servers=bootstrap_servers,
            username=username,
            password=password
        )

    def topic(self, topic: str) -> KafkaConnector:
        return KafkaConnector(
            source=self,
            topic=topic
        )


class MysqlSource(Source):
    host: str
    port: str
    user: str
    password: str
    database: str

    @staticmethod
    def get(
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
    ) -> 'MysqlSource':
        return MysqlSource(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

    def table(self, table_name) -> MysqlConnector:
        return MysqlConnector(
            source=self,
            table=table_name
        )
    