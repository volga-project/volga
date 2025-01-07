import json
from typing import Dict, Optional

from acsylla import create_cluster, create_statement, Session

from volga.storage.scylla.consts import KEYSPACE, REPLICATION_FACTOR, HOT_FEATURE_TABLE_NAME

from scyllapy import Scylla


class HotFeatureStorageApiBase:

    async def init(self):
        raise NotImplementedError()

    async def insert(self, feature_name: str, keys: Dict, values: Dict):
        raise NotImplementedError()

    async def fetch_latest(self, feature_name: str, keys: Dict) -> Dict:
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()


class ScyllaPyHotFeatureStorageApi(HotFeatureStorageApiBase):

    def __init__(self, contact_points=None):
        if contact_points is None:
            self.contact_points = ['127.0.0.1']
        else:
            self.contact_points = contact_points
        self.scylla = Scylla(self.contact_points)

    async def init(self):
        await self.scylla.startup()

        # create table
        await self.scylla.execute("""
                CREATE KEYSPACE IF NOT EXISTS {}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{}'}}
                """.format(KEYSPACE, REPLICATION_FACTOR))
        await self.scylla.execute(f'USE {KEYSPACE}')
        await self.scylla.execute(
            """
            CREATE TABLE IF NOT EXISTS {} (
                feature_name text,
                keys_json text,
                values_json text,
                PRIMARY KEY (feature_name, keys_json)
            )
            """.format(HOT_FEATURE_TABLE_NAME)
        )

    async def insert(self, feature_name: str, keys: Dict, values: Dict):
        keys_json = json.dumps(keys)
        values_json = json.dumps(values)
        q = f'INSERT INTO {HOT_FEATURE_TABLE_NAME} (feature_name, keys_json, values_json) VALUES (:feature_name, :keys_json, :values_json);'
        params = {'feature_name': feature_name, 'keys_json': keys_json, 'values_json': values_json}
        return await self.scylla.execute(q, params)

    async def fetch_latest(self, feature_name: str, keys: Dict) -> Dict:
        q = f'SELECT * FROM {HOT_FEATURE_TABLE_NAME} WHERE feature_name=:feature_name AND keys_json=:keys_json'
        keys_json = json.dumps(keys)
        params = {'feature_name': feature_name, 'keys_json': keys_json}
        res = await self.scylla.execute(q, params)
        res = res.all()
        assert len(res) <= 1

        if len(res) == 0:
            return {}
        else:
            return res[0]

    async def close(self):
        await self.scylla.shutdown()

    async def _drop_tables(self):
        raise NotImplementedError()


class AcsyllaHotFeatureStorageApi(HotFeatureStorageApiBase):

    # TODO set queue_size_io
    def __init__(self, contact_points=None, num_io_threads: int = 1): # TODO configure num io threads
        if contact_points is None:
            self.contact_points = ['127.0.0.1']
        else:
            self.contact_points = contact_points
        self.session: Session = None
        self.num_io_threads = num_io_threads

    async def init(self):
        cluster = create_cluster(contact_points=self.contact_points, port=9042, num_threads_io=self.num_io_threads)
        self.session = await cluster.create_session()
        await self.session.execute(create_statement("""
                CREATE KEYSPACE IF NOT EXISTS {}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{}'}}
                """.format(KEYSPACE, REPLICATION_FACTOR)))
        await self.session.execute(create_statement(f'USE {KEYSPACE}'))
        await self.session.execute(create_statement(
            """
            CREATE TABLE IF NOT EXISTS {} (
                feature_name text,
                keys_json text,
                values_json text,
                PRIMARY KEY (feature_name, keys_json)
            )
            """.format(HOT_FEATURE_TABLE_NAME)
        ))

    async def insert(self, feature_name: str, keys: Dict, values: Dict, ts_micro: Optional[int] = None):
        keys_json = json.dumps(keys)
        values_json = json.dumps(values)
        if ts_micro is None:
            q = f'INSERT INTO {HOT_FEATURE_TABLE_NAME} (feature_name, keys_json, values_json) VALUES (?, ?, ?)'
        else:
            q = f'INSERT INTO {HOT_FEATURE_TABLE_NAME} (feature_name, keys_json, values_json) VALUES (?, ?, ?) USING TIMESTAMP {ts_micro}'
        statement = create_statement(q, parameters=3)
        statement.bind_list([feature_name, keys_json, values_json])

        res = await self.session.execute(statement)
        return res

    async def fetch_latest(self, feature_name: str, keys: Dict) -> Dict:
        keys_json = json.dumps(keys)
        q = f'SELECT * FROM {HOT_FEATURE_TABLE_NAME} WHERE feature_name=? AND keys_json=?'
        statement = create_statement(q, parameters=2)
        statement.bind_list([feature_name, keys_json])
        res = await self.session.execute(statement)
        assert res.count() <= 1

        if res.count() == 0:
            return {}
        else:
            return res.first().as_dict()

    async def close(self):
        if self.session is not None:
            await self.session.close()

    async def _drop_tables(self):
        q = f'DROP TABLE {HOT_FEATURE_TABLE_NAME}'
        statement = create_statement(q)
        await self.session.execute(statement)
