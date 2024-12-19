from typing import Dict, Any

from volga.storage.cassandra.api import fetch_latest
from volga.storage.cassandra.connection import create_session, sync_tables


# TODO abstract data connector
class DataService:

    _instance = None

    def __init__(self):
        create_session()
        sync_tables()

    @staticmethod
    def init():
        if DataService._instance is None:
            DataService._instance = DataService()

    @staticmethod
    def fetch_latest(feature_name: str, keys: Dict[str, Any]) -> Dict:
        assert DataService._instance is not None
        return fetch_latest(feature_name, keys)

