from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from volga.storage.cassandra.consts import KEYSPACE


class HotFeature(Model):
    __table_name__ = 'hot_features'
    __keyspace__ = KEYSPACE
    feature_name = columns.Text(primary_key=True)
    keys_json = columns.Text(primary_key=True)
    values_json = columns.Text()
