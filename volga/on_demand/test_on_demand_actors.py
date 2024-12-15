import asyncio
import json
import time
import unittest
from typing import List

import aiohttp
import ray

from volga.on_demand.actors.coordinator import OnDemandCoordinator
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG


class TestOnDemandActors(unittest.TestCase):

    async def _fetch(self, session, url) -> List:
        async with session.get(url) as response:
            return await response.text()

    async def _fetch_many(self, url, num_requests) -> List:
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(self._fetch(session, url)) for _ in range(num_requests)]
            responses = await asyncio.gather(*tasks)
            return responses

    def test_round_robin(self):
        config = DEFAULT_ON_DEMAND_CONFIG
        config.num_workers_per_node = 5
        config.max_ongoing_requests_per_worker = 999999
        num_requests = 1000

        # this makes sure we do not mess order due to workers buffering work
        assert num_requests < config.num_workers_per_node * config.max_ongoing_requests_per_worker

        url = f'http://127.0.0.1:{config.proxy_port}/fetch_features'

        with ray.init():
            coordinator = OnDemandCoordinator.remote(config)
            _proxy_per_node, _workers_per_node = ray.get(coordinator.start_actors.remote()) # no get - it's blocking

            time.sleep(1)

            loop = asyncio.new_event_loop()
            results = loop.run_until_complete(self._fetch_many(url, num_requests))
            worker_ids = set(list(map(lambda s: json.loads(s)['res'][1], results)))
            assert len(worker_ids) == config.num_workers_per_node

            print('assert ok')



if __name__ == '__main__':
    t = TestOnDemandActors()
    t.test_round_robin()