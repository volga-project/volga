import asyncio
import logging
import socket
import time
from typing import List, Optional, Dict, Any, Tuple, Callable

import uvicorn
from pydantic_core import from_json

import ray.serve

from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand import OnDemandRequest, OnDemandResponse, OnDemandSpec, FeatureValue
from volga.on_demand.on_demand_config import OnDemandConfig
from fastapi import FastAPI, APIRouter

from volga.on_demand.stats import ON_DEMAND_QPS_STATS_CONFIG, ON_DEMAND_SERVER_LATENCY_CONFIG, \
    ON_DEMAND_DB_LATENCY_CONFIG
from volga.stats.stats_manager import CounterStats, HistogramStats, StatsUpdate, ICollectStats
from volga.streaming.common.utils import now_ts_ms

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

        self._on_demand_specs = {}

        # stats
        self.qps_stats = CounterStats.create(ON_DEMAND_QPS_STATS_CONFIG)
        self.latency_stats = HistogramStats.create(ON_DEMAND_SERVER_LATENCY_CONFIG)
        self.db_latency_stats = HistogramStats.create(ON_DEMAND_DB_LATENCY_CONFIG)

    async def init(self):
        await DataService.init(self.config.data_service_config)

    def register_on_demand_specs(self, specs: List[OnDemandSpec]):
        for spec in specs:
            self._on_demand_specs[spec.feature_name] = spec

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
        on_demand_request = OnDemandRequest(**from_json(request_json))
        return await self._do_work(on_demand_request)

    # handle request (can contain multiple features) - either serve directly or execute udf per requested feature
    async def _do_work(self, request: OnDemandRequest, retries: int = 3) -> OnDemandResponse:
        feature_values = {}

        num_attempts = 0
        while True:
            futs = []
            for arg in request.args:
                if arg.serve_or_udf:
                    futs.append(self._fetch(arg.feature_name, arg.keys))
                else:
                    futs.append(self._fetch_and_udf(arg.feature_name, arg.dep_features_keys, arg.udf_args))

            start_ts = time.perf_counter()
            feature_values_list = await asyncio.gather(*futs)
            latency_ms = int((time.perf_counter() - start_ts) * 1000)

            has_empty = False
            for fv in feature_values_list:
                if len(fv.keys) == 0:
                    # retry
                    has_empty = True
                    break # inner
            if has_empty:
                if num_attempts >= retries:
                    raise RuntimeError('Max retries fetching non-empty results')
                else:
                    num_attempts += 1
                    await asyncio.sleep(0.5)
            else:
                # report metrics on success
                self.qps_stats.inc()
                self.latency_stats.observe(latency_ms, now_ts_ms())
                break

        for i in range(len(request.args)):
            feature_name = request.args[i].feature_name
            feature_value = feature_values_list[i]
            feature_values[feature_name] = feature_value

        return OnDemandResponse(
            feature_values=feature_values,
            server_id=self.server_id
        )

    # fetches value in storage and returns
    async def _fetch(self, feature_name: str, keys: Dict[str, Any]) -> FeatureValue:
        start_ts = time.perf_counter()
        feature_value_raw = await DataService.fetch_latest(feature_name=feature_name, keys=keys)

        latency_ms = int((time.perf_counter() - start_ts) * 1000)
        self.db_latency_stats.observe(latency_ms, now_ts_ms())
        return FeatureValue.from_raw(feature_value_raw)

    # fetches dep feature values, performs udf on them + udf_args and returns
    async def _fetch_and_udf(self, feature_name: str, dep_features_keys: List[Tuple[str, Dict[str, Any]]], udf_args: Optional[Dict]) -> FeatureValue:
        spec = self._on_demand_specs[feature_name]
        futs = []
        for (dep_feature_name, keys) in dep_features_keys:
            futs.append(self._fetch(feature_name=dep_feature_name, keys=keys))
        feature_values = await asyncio.gather(*futs)

        # check if we have empty results
        # TODO indicate?
        for fv in feature_values:
            if len(fv.keys) == 0:
                return FeatureValue(keys={}, values={})

        udf = spec.udf
        udf_kwargs = {}

        # map feature args to fetched feature values
        for feature_arg_name in spec.udf_dep_features_args_names:
            dep_feature_name = spec.udf_dep_features_args_names[feature_arg_name]
            # find index to look up result from list of futures
            index = -1
            for i in range(len(dep_features_keys)):
                if dep_features_keys[i][0] == dep_feature_name:
                    index = i
                    break
            assert index >= 0
            dep_feature_value = feature_values[index]
            udf_kwargs[feature_arg_name] = dep_feature_value

        # add udf args
        assert (spec.udf_args_names is None) == (udf_args is None)
        if udf_args is not None:
            assert spec.udf_args_names is not None
            for extra_arg_name in spec.udf_args_names:
                udf_kwargs[extra_arg_name] = udf_args[extra_arg_name]

        return self._execute_udf(udf, **udf_kwargs)

    # TODO add option for threadpool, processpool and ray actor pool
    def _execute_udf(self, udf: Callable, **udf_kwargs) -> FeatureValue:
        return udf(**udf_kwargs)

    def collect_stats(self) -> List[StatsUpdate]:
        return [self.qps_stats.collect(), self.latency_stats.collect(), self.db_latency_stats.collect()]
