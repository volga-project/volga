import asyncio
from pprint import pprint
from typing import List

from aiohttp import ClientSession

from volga.on_demand.actors.server import API_ROUTE
from volga.on_demand.models import OnDemandRequest, OnDemandResponse


# Example request: http://host/on_demand_compute/{"target_features": ["simple_feature"], "feature_keys": {"simple_feature": [{"id": "test-id-0"}, {"id": "test-id-1"}, {"id": "test-id-2"}]}, "query_args": null, "udf_args": {"simple_feature": {"multiplier": 2.0}}}
class OnDemandClient:
    def __init__(self, url_base: str):
        self.url = f'{url_base}/{API_ROUTE}/'
        self._session: ClientSession | None = None

    async def _ensure_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession()
        return self._session

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def request(self, request: OnDemandRequest) -> OnDemandResponse:
        session = await self._ensure_session()
        url = self.url + request.json()
        async with session.get(url) as response:
            try:
                json_response = await response.json()
            except Exception as e:
                raw = await response.read()
                raise Exception(f'Unable to parse json response: {raw}')
            
            return OnDemandResponse.parse_obj(json_response)

    async def request_many(self, requests: List[OnDemandRequest]) -> List[OnDemandResponse]:
        tasks = [
            asyncio.ensure_future(self.request(request)) 
            for request in requests
        ]
        return await asyncio.gather(*tasks)