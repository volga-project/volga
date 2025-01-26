from typing import Dict, Any

from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi, ScyllaPyHotFeatureStorageApi


# TODO abstract data connector
class DataService:

    _instance = None

    def __init__(self, data_service_config: Dict):
        assert 'scylla' in data_service_config
        contact_points = data_service_config['scylla']['contact_points']

        self.api = ScyllaPyHotFeatureStorageApi(contact_points=contact_points)
        # self.api = AcsyllaHotFeatureStorageApi(contact_points=contact_points)

    @staticmethod
    async def init(data_service_config: Dict):
        if DataService._instance is None:
            DataService._instance = DataService(data_service_config)
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
    async def _cleanup_db(data_service_config: Dict):
        await DataService.init(data_service_config)
        await DataService._drop_tables()