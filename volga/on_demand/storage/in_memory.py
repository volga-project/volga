from typing import Dict, Any, Callable, Optional, List

from volga.storage.common.in_memory_actor import InMemoryCacheActor, CACHE_ACTOR_NAME
from volga.on_demand.storage.data_connector import OnDemandDataConnector
from decimal import Decimal

class InMemoryActorOnDemandDataConnector(OnDemandDataConnector):

    def __init__(self):
        self.cache_actor = None
        
    async def init(self):
        """Initialize the Scylla connection"""
        self.cache_actor = InMemoryCacheActor.options(name=CACHE_ACTOR_NAME, get_if_exists=True).remote()
        await self.cache_actor.ready.remote()
        
    def query_dict(self) -> Dict[str, Callable]:
        return {
            'latest': self.fetch_latest,
            'range': self.fetch_range,
        }
        
    async def fetch_latest(
        self, 
        feature_name: str, 
        keys: Dict[str, Any]
    ) -> Any:
        # returns list if same timestamps
        latest_data = await self.cache_actor.get_latest.remote(feature_name, keys)
        return latest_data[-1]
        
    async def fetch_range(
        self,
        feature_name: str,
        keys: Dict[str, Any],
        start: Optional[Decimal],
        end: Optional[Decimal]
    ) -> List[Any]:
        return await self.cache_actor.get_range.remote(feature_name, keys, start, end)
        
    async def close(self):
        pass