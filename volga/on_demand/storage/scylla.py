from typing import Dict, Any, Callable, Optional, List
from datetime import datetime
import json

from volga.storage.scylla.api import ScyllaPyHotFeatureStorageApi
from volga.api.feature import FeatureRepository
from volga.on_demand.storage.data_connector import OnDemandDataConnector

class OnDemandScyllaConnector(OnDemandDataConnector):
    def __init__(self, contact_points: Optional[list[str]] = None):
        self.api = ScyllaPyHotFeatureStorageApi(contact_points=contact_points)

        
    async def init(self):
        await self.api.init()
        
    def query_dict(self) -> Dict[str, Callable]:
        return {
            'latest': self.fetch_latest,
            # 'range': self.fetch_range, # TODO: implement this
        }
        
    async def fetch_latest(
        self, 
        feature_name: str, 
        keys: List[Dict[str, Any]]
    ) -> List[Optional[Dict]]:
        raw_data_list = await self.api.get_latest(feature_name, keys)
        
        # cast to list of lists for compatibility with OnDemandDataConnector
        res = [[self._parse_raw_data(feature_name, d)] for d in raw_data_list]
        return res
        
    def _parse_raw_data(self, feature_name: str, raw_data: Optional[Dict[str, Any]]) -> Optional[Dict]:
        if raw_data is None:
            return None

        feature = FeatureRepository.get_feature(feature_name) # TODO init once
        output_type = feature.output_type
        
        # Parse JSON data
        keys_dict = json.loads(raw_data['keys_json'])
        values_dict = json.loads(raw_data['values_json'])
        
        timestamp_field = output_type._entity_metadata.schema().timestamp

        # Combine keys and values
        output_dict = {**keys_dict, **values_dict}
        if timestamp_field in raw_data:
            output_dict[timestamp_field] = raw_data[timestamp_field]
        else:
            # use fetch time by default
            output_dict[timestamp_field] = datetime.now()
            
        # Create entity instance
        return output_dict
        
    async def close(self):
        await self.api.close()