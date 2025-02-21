import asyncio
from typing import List

from aiohttp import ClientSession

from volga.on_demand.actors.server import API_ROUTE
from volga.on_demand.models import OnDemandRequest, OnDemandResponse


# Example request: http://base/on_demand_compute/{"args": [{"feature_name": "test_feature", "serve_or_udf": true, "keys": {"key": "key_0"}, "dep_features_keys": null, "udf_args": null}]}
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
            json_response = await response.json()
            return OnDemandResponse.parse_obj(json_response)

    async def request_many(self, requests: List[OnDemandRequest]) -> List[OnDemandResponse]:
        tasks = [
            asyncio.ensure_future(self.request(request)) 
            for request in requests
        ]
        return await asyncio.gather(*tasks)