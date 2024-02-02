from typing import TypeVar

from pydantic import BaseModel

from volga.data.api.consts import CONNECTORS_ATTR
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


# produces connections
class Source(BaseModel):

    @staticmethod
    def get(*args, **kwargs) -> 'Source':
        raise NotImplementedError()


# decorator to add source connection to a dataset
def source(conn: Connector, tag: str):
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