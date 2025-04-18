import json
import asyncio
from typing import Dict, List, Optional, Any

import redis.asyncio as redis

from volga.storage.redis.consts import HOT_FEATURE_PREFIX


class RedisFeatureStorageApiBase:

    async def init(self):
        raise NotImplementedError()

    async def insert(self, feature_name: str, keys: Dict, values: Dict):
        raise NotImplementedError()

    async def get_latest(self, feature_name: str, keys: List[Dict]) -> List[Optional[Dict]]:
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()


class RedisHotFeatureStorageApi(RedisFeatureStorageApiBase):

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: Optional[str] = None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.redis = None

    async def init(self):
        self.redis = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True
        )

    def _get_key(self, feature_name: str, keys_dict: Dict) -> str:
        """Create a Redis key from feature name and keys dictionary"""
        keys_json = json.dumps(keys_dict, sort_keys=True)
        return f"{HOT_FEATURE_PREFIX}:{feature_name}:{keys_json}"

    async def insert(self, feature_name: str, keys: Dict, values: Dict):
        if self.redis is None:
            raise RuntimeError("Redis client not initialized. Call init() first.")
        
        redis_key = self._get_key(feature_name, keys)
        data = {
            "feature_name": feature_name,
            "keys_json": json.dumps(keys),
            "values_json": json.dumps(values)
        }
        await self.redis.hset(redis_key, mapping=data)

    async def get_latest(self, feature_name: str, keys: List[Dict]) -> List[Optional[Dict]]:
        if self.redis is None:
            raise RuntimeError("Redis client not initialized. Call init() first.")
        
        async def fetch_single(key_dict):
            redis_key = self._get_key(feature_name, key_dict)
            data = await self.redis.hgetall(redis_key)
            return data if data else {}
        
        results = await asyncio.gather(*[fetch_single(key_dict) for key_dict in keys])
        return results

    async def close(self):
        if self.redis is not None:
            await self.redis.close()

    async def _delete_data(self):
        """Delete all data with the hot feature prefix - for testing purposes"""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized. Call init() first.")
        
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=f"{HOT_FEATURE_PREFIX}:*", count=100)
            if keys:
                await self.redis.delete(*keys)
            if cursor == 0:
                break 