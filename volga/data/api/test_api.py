import datetime
import unittest

from volga.data.api.dataset.dataset import dataset, field, Dataset
from volga.data.api.dataset.pipeline import pipeline
from volga.data.api.source.source import KafkaSource, MysqlSource, source
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.stream.stream_source import StreamSource


class TestApi(unittest.TestCase):

    def test_dataset(self):

        @dataset
        class User:
            user_id: str = field(key=True)
            timestamp: datetime.datetime = field(timestamp=True)
            name: str

        assert isinstance(User, Dataset)
        fields = User._fields
        key_fields = User._key_fields
        timestamp_field = User._timestamp_field

        assert len(fields) == 3
        assert key_fields == ['user_id']
        assert timestamp_field == 'timestamp'

    def test_pipline(self):
        @dataset
        class User:
            user_id: str = field(key=True)
            name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @dataset
        class Order:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            product_name: str
            timestamp: datetime.datetime = field(timestamp=True)

        @dataset
        class UserOrderInfo:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            product_name: str
            user_name: str
            timestamp: datetime.datetime = field(timestamp=True)

            @pipeline(inputs=[User, Order])
            def gen(cls, users: Dataset, orders: Dataset):

                p = users.join(orders, on=['user_id'])
                p = p.filter(lambda x: x != '')
                p = p.dropnull()
                return p

        pipe = UserOrderInfo._pipeline
        assert len(pipe.inputs) == 2

        # print(pipe.terminal_node)
        # print(pipe.terminal_node.out_edges)
        print(pipe.inputs[0].out_edges)
        print(pipe.inputs[1].out_edges)

    def test_source(self):
        kafka = KafkaSource.get(bootstrap_servers='', username='', password='')
        mysql = MysqlSource.get(host='127.0.0.1', port='3306', user='root', password='', database='db')

        @source(kafka.topic('topic'), 'online')
        @source(mysql.table('table'), 'offline')
        @dataset
        class User:
            user_id: str = field(key=True)
            name: str
            timestamp: datetime.datetime = field(timestamp=True)

        sources = User.get_source_connectors()
        assert len(sources) == 2
        assert 'online' in sources
        assert 'offline' in sources

        ctx = StreamingContext()
        kafka_connector = sources['online']
        kc = kafka_connector.to_stream_source(ctx)
        assert isinstance(kc, StreamSource)

        mysql_connector = sources['offline']
        mc = mysql_connector.to_stream_source(ctx)
        assert isinstance(mc, StreamSource)


if __name__ == '__main__':
    t = TestApi()
    t.test_dataset()
    t.test_pipline()
    t.test_source()