import json
from typing import Dict, Any, List, Tuple, Optional

from volga.storage.scylla.models import HotFeature
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.models import Model


def fetch_latest(feature_name: str, keys: Dict[str, Any]) -> Optional[Dict]:
    keys_json = json.dumps(keys)
    try:
        feature = HotFeature.get(feature_name=feature_name, keys_json=keys_json)
        values_json = feature.values_json
        return json.loads(values_json)
    except Model.DoesNotExist:
        return None


def store_many(feature_name: str, features: List[Tuple[Dict[str, Any], Dict[str, Any]]]):
    b = BatchQuery()
    for f in features:
        keys = f[0]
        vals = f[1]
        HotFeature.batch(b).create(feature_name=feature_name, keys_json=json.dumps(keys), values_json=json.dumps(vals))
    b.execute()
