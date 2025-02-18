from typing import Dict, Any
import json

from volga.storage.scylla.api import AcsyllaHotFeatureStorageApi, ScyllaPyHotFeatureStorageApi
from volga.api.feature import FeatureRepository

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
    async def fetch_latest(feature_name: str, keys: Dict[str, Any]) -> Any:
        assert DataService._instance is not None
        # Get raw data from storage
        raw_data = await DataService._instance.api.fetch_latest(feature_name, keys)
        
        # Get feature from repository to determine entity type
        feature = FeatureRepository.get_feature(feature_name)
        output_type = feature.output_type
        
        # Parse JSON data
        keys_dict = json.loads(raw_data['keys_json'])
        values_dict = json.loads(raw_data['values_json'])
        # Get timestamp field name from entity class fields
        timestamp_field = next(
            (field_name for field_name, field in feature.output_type._fields.items()
             if field.timestamp),
            'timestamp'  # Default to 'timestamp' if not found
        )
        assert timestamp_field in raw_data
        ts = raw_data[timestamp_field]
        
        
        # Combine keys and values
        output_dict = {**keys_dict, **values_dict, timestamp_field: ts}
        
        # Create entity instance
        return output_type(**output_dict)

    # funcs below are for testing only
    @staticmethod
    async def _drop_tables():
        return await DataService._instance.api._drop_tables()

    @staticmethod
    async def _cleanup_db(data_service_config: Dict):
        await DataService.init(data_service_config)
        await DataService._drop_tables()