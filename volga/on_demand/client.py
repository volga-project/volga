import asyncio
import json
from typing import Optional, List

from aiohttp import ClientSession

from volga.on_demand.actors.server import API_ROUTE
from volga.on_demand.on_demand import OnDemandRequest, OnDemandResponse
from volga.on_demand.on_demand_config import OnDemandConfig


class OnDemandClient:

    def __init__(self, config: OnDemandConfig):
        self.config = config
        self.url_base = f'http://{config.client_url}:{config.server_port}/{API_ROUTE}/'

    async def request(self, request: OnDemandRequest, session: Optional[ClientSession] = None) -> OnDemandResponse:
        if session is None:
            session = ClientSession()
        url = self.url_base + request.json()
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
