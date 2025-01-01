import asyncio
import concurrent.futures
import functools
from typing import Dict, Any, Optional, List, Tuple

import ray

from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand import OnDemandSpec, OnDemandRequest, FeatureValue, OnDemandResponse
from volga.on_demand.on_demand_config import OnDemandConfig


# Used either for
# 1. Simply serving feature (fetch data from storage and pass back)
# 2. Performing udf (fetch -> udf -> pass back)
@ray.remote
class OnDemandWorker:

    def __init__(self, host_node_id: int, worker_id: int, config: OnDemandConfig):
        self.config = config
        self.host_node_id = host_node_id
        self.worker_id = worker_id
        self._on_demand_specs = {}
        self._on_demand_udf_threads = concurrent.futures.ThreadPoolExecutor(max_workers=config.num_udf_threads_per_worker)

    async def init(self):
        await DataService.init()

    def register_on_demand_specs(self, specs: List[OnDemandSpec]):
        for spec in specs:
            self._on_demand_specs[spec.feature_name] = spec

    # handle batch request - either serve or udf per requested feature
    async def do_work(self, request: OnDemandRequest) -> OnDemandResponse:
        feature_values = {}
        futs = []
        for arg in request.args:
            if arg.serve_or_udf:
                futs.append(self._serve(arg.feature_name, arg.keys))
            else:
                futs.append(self._udf(arg.feature_name, arg.dep_features_keys, arg.udf_args))

        feature_values_list = await asyncio.gather(*futs)

        for i in range(len(request.args)):
            feature_name = request.args[i].feature_name
            feature_value = feature_values_list[i]
            feature_values[feature_name] = feature_value

        return OnDemandResponse(
            feature_values=feature_values,
            worker_id=self.worker_id
        )

    # fetches value in storage and returns
    async def _serve(self, feature_name: str, keys: Dict[str, Any]) -> FeatureValue:
        feature_value_raw = await DataService.fetch_latest(feature_name=feature_name, keys=keys)
        return FeatureValue.from_raw(feature_value_raw)

    # fetches dep feature values, performs udf on them + udf_args and returns
    async def _udf(self, feature_name: str, dep_features_keys: List[Tuple[str, Dict[str, Any]]], udf_args: Optional[Dict]) -> FeatureValue:
        spec = self._on_demand_specs[feature_name]
        futs = []
        for (dep_feature_name, keys) in dep_features_keys:
            futs.append(DataService.fetch_latest(feature_name=dep_feature_name, keys=keys))
        feature_values_raw = await asyncio.gather(*futs)
        feature_values = list(map(
            lambda v: FeatureValue.from_raw(v),
            feature_values_raw
        ))
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

        # asynchronously perform udf on a thread pool so we do not block main event loop
        # TODO record perf
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(self._on_demand_udf_threads, functools.partial(udf, **udf_kwargs))
        return res
