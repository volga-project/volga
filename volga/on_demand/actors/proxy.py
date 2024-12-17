import asyncio
import json
import logging
import socket
import time
from typing import List, Optional, Dict, Any

import ray
import uvicorn
from ray.actor import ActorHandle

import ray.serve

from volga.on_demand.on_demand_config import OnDemandConfig
from fastapi import FastAPI, APIRouter

logger = logging.getLogger('ray')


@ray.remote(max_concurrency=999)
class OnDemandProxy:

    def __init__(self, config: OnDemandConfig, workers: List[ActorHandle]):
        self.workers = workers
        self.config = config
        self.host = '0.0.0.0'
        self.app = FastAPI(debug=True)
        router = APIRouter()

        # TODO move route init to separate class
        router.add_api_route('/fetch_features/{feature_name}/{keys_json}', endpoint=self.proxy_to_worker, methods=["GET"])
        self.app.include_router(router)
        self.requests_per_worker = {worker_id: 0 for worker_id in range(len(workers))}
        self.last_ready_worker_id = -1

        self.lock = asyncio.Lock()

    async def proxy_to_worker(self, feature_name: str, keys_json: str):
        worker_id = await self._get_next_ready_worker_id(timeout_s=2)
        if worker_id is None:
            # timeout - all workers busy
            logger.info('[On Demand] All workers are busy')
            return {} # TODO proper indicate busy workers to API

        worker = self.workers[worker_id]
        keys = json.loads(keys_json)
        feature_values = await worker.do_work.remote(feature_name, keys)
        await self._release_worker(worker_id)
        return {'feature_name': feature_name, 'keys': keys, 'values': feature_values, 'worker_id': worker_id}

    # round-robins until there is a worker with available work slots
    async def _get_next_ready_worker_id(self, timeout_s: int) -> Optional[int]:
        start_ts = time.time()

        while time.time() - start_ts < timeout_s:
            await self.lock.acquire()
            num_skipped = 0
            all_busy = False

            next_worker_id = (self.last_ready_worker_id + 1)%len(self.workers)
            while self.requests_per_worker[next_worker_id] >= self.config.max_ongoing_requests_per_worker:
                next_worker_id = (next_worker_id + 1)%len(self.workers)
                num_skipped += 1
                if num_skipped == len(self.workers):
                    # all busy
                    all_busy = True
                    break
            if all_busy:
                self.lock.release()
                continue
            self.last_ready_worker_id = next_worker_id
            self.requests_per_worker[next_worker_id] += 1
            self.lock.release()
            return next_worker_id

        return None

    async def _release_worker(self, worker_id: int):
        await self.lock.acquire()
        self.requests_per_worker[worker_id] -= 1
        self.lock.release()

    async def run(self):
        sock = socket.socket()
        try:
            sock.bind((self.host, self.config.proxy_port))
        except OSError:
            raise ValueError(f'Failed to bind HTTP proxy to {self.host}:{self.config.proxy_port}')
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.config.proxy_port,
            loop='uvloop',
            # log_level='warning',
        )
        uvicorn_server = uvicorn.Server(config=config)
        await uvicorn_server.serve(sockets=[sock])
