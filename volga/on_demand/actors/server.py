import asyncio
import logging
import socket
import time
from typing import List, Optional, Dict, Any, Type
import importlib

import uvicorn
from pydantic_core import from_json
from fastapi import FastAPI, APIRouter

from volga.on_demand.models import OnDemandRequest, OnDemandResponse
from volga.on_demand.config import OnDemandConfig
from volga.on_demand.stats import (
    ON_DEMAND_QPS_STATS_CONFIG,
    ON_DEMAND_SERVER_LATENCY_CONFIG,
    ON_DEMAND_DB_LATENCY_CONFIG
)
from volga.stats.stats_manager import CounterStats, HistogramStats, StatsUpdate, ICollectStats
from volga.streaming.common.utils import now_ts_ms
from volga.api.feature import FeatureRepository, Feature
from volga.on_demand.executor import OnDemandExecutor

import ray


logger = logging.getLogger('ray')

API_ROUTE = 'on_demand_compute'

@ray.remote(max_concurrency=9999)
class OnDemandServer(ICollectStats):
    def __init__(self, node_id: str, server_id: int, config: OnDemandConfig):
        self.node_id = node_id
        self.server_id = server_id
        self.config = config
        self.host = '0.0.0.0'
        self.app = FastAPI(debug=True)
        router = APIRouter()

        # TODO move route init to separate class
        api_route = f'/{API_ROUTE}/' + '{request_json}'
        router.add_api_route(api_route, endpoint=self._serve, methods=["GET"])
        self.app.include_router(router)

        # stats
        self.qps_stats = CounterStats.create(ON_DEMAND_QPS_STATS_CONFIG)
        self.latency_stats = HistogramStats.create(ON_DEMAND_SERVER_LATENCY_CONFIG)
        self.db_latency_stats = HistogramStats.create(ON_DEMAND_DB_LATENCY_CONFIG)

        self.data_connector = self.config.data_connector.connector_class(**self.config.data_connector.connector_args)
        
        self.executor = OnDemandExecutor(
            data_connector=self.data_connector,
            db_stats=self.db_latency_stats
        )

    def register_features(self, features: Dict[str, Feature]):
        self.executor.register_features(features)

    async def init(self):
        await self.data_connector.init()

    async def run(self):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, 15, 1)
        try:
            sock.bind((self.host, self.config.server_port))
        except OSError:
            raise ValueError(f'Failed to bind HTTP proxy to {self.host}:{self.config.server_port}')
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.config.server_port,
            loop='uvloop',
            log_level='critical',
        )
        uvicorn_server = uvicorn.Server(config=config)
        await uvicorn_server.serve(sockets=[sock])

    async def _serve(self, request_json: str) -> OnDemandResponse:
        request = OnDemandRequest(**from_json(request_json))
        
        self.qps_stats.inc()
        start_ts = time.perf_counter()
        
        results = await self.executor.execute(request)

        latency_ms = int((time.perf_counter() - start_ts) * 1000)
        self.latency_stats.observe(latency_ms, now_ts_ms())

        return OnDemandResponse(
            results=results,
            server_id=int(self.server_id)
        )

    def collect_stats(self) -> List[StatsUpdate]:
        return [
            self.qps_stats.collect(),
            self.latency_stats.collect(),
            self.db_latency_stats.collect()
        ]
    
    async def close(self):
        await self.data_connector.close()
