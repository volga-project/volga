import asyncio
import functools
import time
import unittest
from asyncio import FIRST_COMPLETED

import ray
from aiohttp import ClientSession

from volga.on_demand.actors.coordinator import OnDemandCoordinator
from volga.on_demand.client import OnDemandClient
from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand import OnDemandRequest, OnDemandArgs
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG
from volga.on_demand.testing_utils import TEST_FEATURE_NAME, create_sample_feature_data, sample_key_value


class TestOnDemandPerf(unittest.TestCase):

    def test_local(self):
        num_keys = 10000
        config = DEFAULT_ON_DEMAND_CONFIG

        loop = asyncio.get_event_loop()

        loop.run_until_complete(DataService._cleanup_db(config.data_service_config))
        create_sample_feature_data(num_keys)

        coordinator = OnDemandCoordinator.remote(config)
        ray.get(coordinator.start.remote())

        time.sleep(1)  # wait for on-demand workers to spin up

        client = OnDemandClient(config)

        i = 0
        tasks = set()
        max_tasks = 1000
        session = ClientSession()
        last_done_ts = [time.time()]
        while True:
            if len(tasks) == max_tasks:
                # TODO indicate
                # print('max tasks reached')
                loop.run_until_complete(asyncio.wait(tasks, return_when=FIRST_COMPLETED))
                assert len(tasks) < max_tasks

            keys, _ = sample_key_value(i)
            request = OnDemandRequest(args=[OnDemandArgs(feature_name=TEST_FEATURE_NAME, serve_or_udf=True, keys=keys)])
            i = (i + 1) % num_keys
            task = loop.create_task(client.request(request, session))
            tasks.add(task)

            def _done(_task, _i, _last_done_ts):
                tasks.discard(_task)
                n = 5000
                if _i%n == 0:
                    dur = time.time() - _last_done_ts[0]
                    qps = n/dur
                    print(f'{_i} done, {qps} r/s')
                    _last_done_ts[0] = time.time()

            task.add_done_callback(functools.partial(_done, _i=i, _last_done_ts=last_done_ts))


if __name__ == '__main__':
    t = TestOnDemandPerf()
    t.test_local()