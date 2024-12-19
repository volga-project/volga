import json
from typing import Dict, Any, List, Tuple, Optional

from volga.storage.cassandra.models import HotFeature
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.models import Model
from cassandra.cluster import Session, ResponseFuture


def fetch_latest(feature_name: str, keys: Dict[str, Any]) -> Optional[Dict]:
    keys_json = json.dumps(keys)
    try:
        feature = HotFeature.get(feature_name=feature_name, keys_json=keys_json)
        values_json = feature.values_json
        return json.loads(values_json)
    except Model.DoesNotExist:
        return None


def fetch_latest_async(session: Session, feature_name: str, keys: Dict[str, Any]) -> ResponseFuture:
    prep = f'SELECT * FROM {HotFeature.__table_name__} WHERE feature_name=? AND keys_json=?'
    ps = session.prepare(prep)
    keys_json = json.dumps(keys)
    args = [feature_name, keys_json]

    return session.execute_async(ps.bind(args))


def store_many(feature_name: str, features: List[Tuple[Dict[str, Any], Dict[str, Any]]]):
    b = BatchQuery()
    for f in features:
        keys = f[0]
        vals = f[1]
        HotFeature.batch(b).create(feature_name=feature_name, keys_json=json.dumps(keys), values_json=json.dumps(vals))
    b.execute()


# https://stackoverflow.com/questions/17885238/how-to-multi-insert-rows-in-cassandra
def store_many_async(session: Session, feature_name: str, features: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> ResponseFuture:
    prep = 'BEGIN BATCH\n'
    for _ in range(len(features)):
        prep += f'INSERT INTO {HotFeature.__table_name__} (feature_name, keys_json, values_json) VALUES (?, ?, ?);'
    prep += 'APPLY BATCH'
    ps = session.prepare(prep)
    args = []
    for feature in features:
        args.append(feature_name)
        keys = feature[0]
        vals = feature[1]
        keys_json = json.dumps(keys)
        values_json = json.dumps(vals)
        args.append(keys_json)
        args.append(values_json)

    return session.execute_async(ps.bind(args))
