import cassandra

from volga.storage.scylla.consts import REPLICATION_FACTOR, KEYSPACE
from volga.storage.scylla.models import HotFeature
from cassandra.cluster import Cluster
from cassandra.cqlengine.connection import register_connection, set_default_connection
from cassandra.cqlengine.management import sync_table

def create_session():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS {}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{}'}}
        """.format(KEYSPACE, REPLICATION_FACTOR)
    )
    session.execute(f'USE {KEYSPACE}')
    register_connection(str(session), session=session)
    set_default_connection(str(session))


def sync_tables():
    try:
        sync_table(HotFeature)
    except cassandra.AlreadyExists:
        pass
