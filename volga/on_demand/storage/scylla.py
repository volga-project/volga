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
        """Initialize the Scylla connection"""
        await self.api.init()
        
    def query_dict(self) -> Dict[str, Callable]:
        return {
            'latest': self.fetch_latest,
            # 'range': self.fetch_range,
        }
        
    async def fetch_latest(
        self, 
        feature_name: str, 
        keys: Dict[str, Any]
    ) -> Any:
        """Fetch latest value for a feature"""
        raw_data = await self.api.fetch_latest(feature_name, keys)
        
        if not raw_data:
            raise ValueError(f"No data found for feature {feature_name} with keys {keys}")
            
        return self._parse_entity(feature_name, raw_data)
        
    # async def fetch_range(
    #     self,
    #     feature_name: str,
    #     keys: Dict[str, Any],
    #     start_time: datetime,
    #     end_time: datetime
    # ) -> List[Any]:
    #     """Fetch feature values within a time range"""
    #     raw_data = await self.api.fetch_range(
    #         feature_name, 
    #         keys,
    #         start_time,
    #         end_time
    #     )
        
    #     return [self._parse_entity(feature_name, item) for item in raw_data]
        
    def _parse_entity(self, feature_name: str, raw_data: Dict[str, Any]) -> Any:
        """Parse raw data into an entity instance"""
        feature = FeatureRepository.get_feature(feature_name)
        output_type = feature.output_type
        
        # Parse JSON data
        keys_dict = json.loads(raw_data['keys_json'])
        values_dict = json.loads(raw_data['values_json'])
        
        # Get timestamp field name from entity class fields
        timestamp_field = next(
            (field_name for field_name, field in output_type._entity._fields.items() 
             if field.timestamp),
            'timestamp'  # Default to 'timestamp' if not found
        )
        
        # Combine keys and values
        output_dict = {**keys_dict, **values_dict}
        if timestamp_field in raw_data:
            output_dict[timestamp_field] = raw_data[timestamp_field]
            
        # Create entity instance
        return output_type(**output_dict)
        
    async def close(self):
        """Close the Scylla connection"""
        await self.api.close()