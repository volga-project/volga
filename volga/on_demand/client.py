import asyncio
import json
from typing import Optional, List

import aiohttp
from aiohttp import ClientSession

from volga.on_demand.actors.server import API_ROUTE
from volga.on_demand.on_demand import OnDemandRequest, OnDemandResponse
from volga.on_demand.on_demand_config import OnDemandConfig


# Example request: http://base/on_demand_compute/{"args": [{"feature_name": "test_feature", "serve_or_udf": true, "keys": {"key": "key_0"}, "dep_features_keys": null, "udf_args": null}]}
class OnDemandClient:

    def __init__(self, config: Optional[OnDemandConfig], url_base: Optional[str] = None):
        if url_base is None:
            assert config is not None
            self.url = f'http://{config.client_url}:{config.server_port}/{API_ROUTE}/'
        else:
            self.url = f'{url_base}/{API_ROUTE}/'


    async def request_no_keepalive(self, request: OnDemandRequest) -> OnDemandResponse:
        url = self.url + request.json()
        async with aiohttp.request('GET', url) as response:
            raw = await response.text()
            await asyncio.sleep(0.1)
            return OnDemandResponse(**json.loads(raw))

    async def request(self, request: OnDemandRequest, session: Optional[ClientSession] = None) -> OnDemandResponse:
        if session is None:
            session = ClientSession()
        url = self.url + request.json()
        async with session.get(url) as response:
            raw = await response.text()
            return OnDemandResponse(**json.loads(raw))

    async def request_many(self, requests: List[OnDemandRequest], session: Optional[ClientSession] = None) -> List[OnDemandResponse]:
        if session is None:
            session = ClientSession()
        async with session:
            tasks = [asyncio.ensure_future(self.request(request, session)) for request in requests]
            responses = await asyncio.gather(*tasks)
            return responses
