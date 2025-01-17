import asyncio
import functools
from asyncio import FIRST_COMPLETED
from threading import Thread

from aiohttp import ClientSession
from fastapi import FastAPI, APIRouter
import uvicorn, os, time

from volga.on_demand.actors.proxy import API_ROUTE
from volga.on_demand.client import OnDemandClient
from volga.on_demand.on_demand import OnDemandRequest, OnDemandArgs, OnDemandResponse
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG
from volga.on_demand.testing_utils import sample_key_value, TEST_FEATURE_NAME

app = FastAPI(debug=True)
router = APIRouter()

async def resp(request_json: str):
    # return {'message': f'This is a delayed response!- {os.getpid()}'}
    print(os.getpid())
    return OnDemandResponse(worker_id=0, feature_values={})

# TODO move route init to separate class
api_route = f'/{API_ROUTE}/' + '{request_json}'
router.add_api_route(api_route, endpoint=resp, methods=["GET"])


def request_loop():
    config = DEFAULT_ON_DEMAND_CONFIG
    time.sleep(3)
    client = OnDemandClient(config)
    num_keys = 1000
    # loop.run_until_complete(self._request_loop(client, 1000, num_keys))
    loop = asyncio.get_event_loop()
    i = 0
    tasks = set()
    max_tasks = 1000
    session = ClientSession(loop=loop)
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
            if _i % n == 0:
                dur = time.time() - _last_done_ts[0]
                qps = n / dur
                print(f'{_i} done, {qps} r/s')
                _last_done_ts[0] = time.time()

        task.add_done_callback(functools.partial(_done, _i=i, _last_done_ts=last_done_ts))
        time.sleep(1)

if __name__ == "__main__":
    t = Thread(target=request_loop)
    t.start()
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=DEFAULT_ON_DEMAND_CONFIG.proxy_port,
        reload=False,
        workers=1,
        log_level='critical'
    )



