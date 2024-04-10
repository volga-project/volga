from typing import TypeVar, List, Any

from pydantic import BaseModel

from volga.api.consts import CONNECTORS_ATTR
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


class MockOnlineConnector(Connector):

    def __init__(self, items: List[Any], delay_s: int):
        self.items = items
        self.delay_s = delay_s

    def to_stream_source(self, ctx: StreamingContext) -> StreamSource:
        return ctx.from_delayed_collection(self.items, delay_s=self.delay_s)


# produces connections
class Source(BaseModel):

    @staticmethod
    def get(*args, **kwargs) -> 'Source':
        raise NotImplementedError()


# decorator to add source connection to a dataset
def source(conn: Connector, tag: str = 'default'):
    if not isinstance(conn, Connector):
        raise TypeError('Expected Connector type')

    def decorator(dataset_cls: T):
        connectors = getattr(dataset_cls, CONNECTORS_ATTR, {})
        if tag in connectors:
            raise ValueError(f'Duplicate {tag} for source {conn}')
        connectors[tag] = conn
        setattr(dataset_cls, CONNECTORS_ATTR, connectors)
        return dataset_cls

    return decorator


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

    @staticmethod
    def mock_with_delayed_items(items: List[Any], delay_s: int) -> MockOnlineConnector:
        return MockOnlineConnector(items, delay_s)


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

    @staticmethod
    def mock_with_items(items: List[Any]) -> MockOfflineConnector:
        return MockOfflineConnector(items)