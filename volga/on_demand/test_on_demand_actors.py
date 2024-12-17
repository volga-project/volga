import asyncio
import json
import time
import unittest
from typing import List

import aiohttp
import ray

from volga.on_demand.actors.coordinator import OnDemandCoordinator
from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG
from volga.storage.scylla.api import store_many


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
        config.num_workers_per_node = 2
        config.max_ongoing_requests_per_worker = 999999
        num_requests = 2

        feature_name = 'test_feature'
        keys = {'key1': '1', 'key2': '2'}
        values = {'val1': 1, 'val2': 2}
        keys_json = json.dumps(keys)

        DataService.init()
        store_many(feature_name, [(keys, values)])

        url = f'http://127.0.0.1:{config.proxy_port}/fetch_features/{feature_name}/{keys_json}'

        with ray.init():
            coordinator = OnDemandCoordinator.remote(config)
            _proxy_per_node, _workers_per_node = ray.get(coordinator.start_actors.remote()) # no get - it's blocking

            time.sleep(1)

            loop = asyncio.new_event_loop()
            results = loop.run_until_complete(self._fetch_many(url, num_requests))
            print(results)
            worker_ids = set(list(map(lambda s: json.loads(s)['worker_id'], results)))
            assert len(worker_ids) == config.num_workers_per_node

            print('assert ok')


if __name__ == '__main__':
    t = TestOnDemandActors()
    t.test_round_robin()