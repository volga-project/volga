import asyncio
import time
import unittest
import ray


from volga.on_demand.actors.coordinator import OnDemandCoordinator
from volga.on_demand.client import OnDemandClient
from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand import OnDemandArgs, OnDemandRequest, OnDemandSpec, FeatureValue, OnDemandResponse
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG


class TestOnDemandActors(unittest.TestCase):

    def test_serve(self):
        self._test_serve_or_udf(True)

    def test_udf(self):
        self._test_serve_or_udf(False)

    def _test_serve_or_udf(self, serve_or_udf: bool):
        config = DEFAULT_ON_DEMAND_CONFIG
        config.num_workers_per_node = 5
        config.max_ongoing_requests_per_worker = 999999
        num_requests = 1000

        # serve
        feature_name = 'test_feature'
        keys = {'key1': '1', 'key2': '2'}
        values = {'val1': 1, 'val2': 2}

        # udf
        main_feature_name = 'udf_feature'
        def _udf_increment_vals(feature_value: FeatureValue, increment) -> FeatureValue:
            feature_value.values['val1'] = feature_value.values['val1'] + increment
            feature_value.values['val2'] = feature_value.values['val2'] + increment
            return feature_value

        increment = 1

        # TODO automatically generate this from udf
        on_demand_spec = OnDemandSpec(
            feature_name=main_feature_name,
            udf=_udf_increment_vals,
            udf_dep_features_args_names={'feature_value': feature_name},
            udf_args_names=['increment']
        )

        if serve_or_udf:
            request = OnDemandRequest(args=[OnDemandArgs(feature_name=feature_name, serve_or_udf=True, keys=keys)])
        else:
            request = OnDemandRequest(args=[OnDemandArgs(
                feature_name=main_feature_name,
                serve_or_udf=False,
                dep_features_keys=[(feature_name, keys)],
                udf_args={'increment': increment}
            )])

        loop = asyncio.get_event_loop()
        loop.run_until_complete(DataService.init())
        loop.run_until_complete(DataService._instance.api.insert(feature_name, keys, values))

        client = OnDemandClient(config)
        requests = [request for _ in range(num_requests)]

        with ray.init():
            coordinator = OnDemandCoordinator.remote(config)
            _proxy_per_node, _workers_per_node = ray.get(coordinator.start_actors.remote())
            if not serve_or_udf:
                # register specs for udf case
                ray.get(coordinator.register_on_demand_specs.remote([on_demand_spec]))

            time.sleep(1)

            loop = asyncio.new_event_loop()
            responses = loop.run_until_complete(client.request_many(requests))
            # worker = list(_workers_per_node.values())[0][0]
            # res = ray.get(worker.do_work.remote(request))
            # print(res)
            worker_ids = set(list(map(lambda r: r.worker_id, responses)))
            if num_requests < config.num_workers_per_node:
                assert len(worker_ids) == num_requests
            else:
                assert len(worker_ids) == config.num_workers_per_node
            for r in responses:
                if serve_or_udf:
                    _keys = r.feature_values[feature_name].keys
                    _values = r.feature_values[feature_name].values
                    assert _keys == keys
                    assert _values == values
                else:
                    _values = r.feature_values[main_feature_name].values
                    assert _values['val1'] == values['val1'] + increment
                    assert _values['val2'] == values['val2'] + increment
            print('assert ok')


if __name__ == '__main__':
    t = TestOnDemandActors()
    t.test_serve()
    t.test_udf()