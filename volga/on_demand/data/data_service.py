from typing import Dict, Any, Optional

from volga.storage.scylla.api import ScyllaPyHotFeatureStorageApi, AcsyllaHotFeatureStorageApi


# TODO abstract data connector
class DataService:

    _instance = None

    def __init__(self):
        # self.api = ScyllaPyHotFeatureStorageApi()
        self.api = AcsyllaHotFeatureStorageApi()

    @staticmethod
    async def init():
        if DataService._instance is None:
            DataService._instance = DataService()
        await DataService._instance.api.init()

    @staticmethod
    async def fetch_latest(feature_name: str, keys: Dict[str, Any]) -> Dict:
        assert DataService._instance is not None
        return await DataService._instance.api.fetch_latest(feature_name, keys)

    # funcs below are for testing only
    @staticmethod
    async def _drop_tables():
        return await DataService._instance.api._drop_tables()

    @staticmethod
    async def _cleanup_db():
        await DataService.init()
        await DataService._drop_tables()