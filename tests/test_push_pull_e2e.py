import asyncio
import time
import unittest
from pathlib import Path

import ray
import yaml

from volga.on_demand.actors.coordinator import OnDemandCoordinator
from volga.on_demand.client import OnDemandClient
from volga.on_demand.data.data_service import DataService
from volga.on_demand.on_demand import OnDemandRequest, OnDemandArgs
from volga.on_demand.on_demand_config import DEFAULT_ON_DEMAND_CONFIG
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.aggregate_function import AggregationType
from volga.streaming.api.message.message import Record
from volga.streaming.api.operators.timestamp_assigner import EventTimeAssigner
from volga.streaming.api.operators.window_operator import SlidingWindowConfig, AggregationsPerWindow
from volga.streaming.runtime.sinks.scylla.scylla_hot_feature_sink_function import ScyllaHotFeatureSinkFunction


# streaming job + on-demand serving/transformations requests
class TestPushPullE2E(unittest.TestCase):

    # simulates scenario where a streaming job writes agg counters to storage and client calls on-demand workers for serving
    def test_counters(self):
        feature_name = 'post_click_count_1m'

        num_users = 20
        num_posts = 100

        push_period_s = 0.5
        agg_window_minutes = 1

        assert push_period_s * 100 < 60 * agg_window_minutes # makes sure we receive expeected results for assertions later

        # cleanup db
        asyncio.get_event_loop().run_until_complete(DataService._cleanup_db())

        # streaming job def
        # this simulates an event where a user clicks all posts simultaneously
        # we repeat it for each user with push_period_s period
        events = []
        for user_id in range(num_users):
            clicks = [{
                'user_id': user_id,
                'post_id': post_id
            } for post_id in range(num_posts)]
            events.append(clicks)

        ray.init(address='auto', ignore_reinit_error=True)

        job_config = yaml.safe_load(
            Path('/Users/anov/IdeaProjects/volga/volga/streaming/runtime/sample-job-config.yaml').read_text())
        ctx = StreamingContext(job_config=job_config)
        # s = ctx.from_collection(events)
        s = ctx.from_periodic_collection(events, push_period_s)
        s = s.timestamp_assigner(EventTimeAssigner(lambda e: time.time()))

        def _extractor(e):
            return {'post_id': e['post_id']}, {'count_1m': e['count_1m']}

        sink_function = ScyllaHotFeatureSinkFunction(
            feature_name=feature_name,
            key_value_extractor=_extractor,
        )

        def _window_output_func(aggs: AggregationsPerWindow, record: Record) -> Record:
            v = aggs
            v['post_id'] = record.value['post_id']
            return Record(
                event_time=record.event_time,
                source_emit_ts=record.source_emit_ts,
                value=v
            )

        s.flat_map(lambda clicks: clicks).key_by(lambda e: e['post_id']).multi_window_agg([SlidingWindowConfig(
            duration=f'{agg_window_minutes}m',
            agg_type=AggregationType.COUNT,
            agg_on_func=(lambda e: e['user_id']),
        )], _window_output_func).sink(sink_function)


        # on-demand compute
        config = DEFAULT_ON_DEMAND_CONFIG
        config.num_workers_per_node = 5
        config.max_ongoing_requests_per_worker = 999999

        coordinator = OnDemandCoordinator.remote(config)
        ray.get(coordinator.start_actors.remote())

        time.sleep(1) # wait for on-demand workers to spin up

        # submit streaming job after on-demand actors are ready
        ctx.submit()

        client = OnDemandClient(config)

        requests = [
            OnDemandRequest(args=[OnDemandArgs(feature_name=feature_name, serve_or_udf=True, keys={'post_id': post_id})])
            for post_id in range(num_posts)
        ]
        counts_by_post_id = {} # count event per post id for each update
        loop = asyncio.get_event_loop()
        start_ts = time.time()
        test_timeout_s = 20
        while True:
            if time.time() - start_ts > test_timeout_s:
                raise RuntimeError('Test timeout')
            responses = loop.run_until_complete(client.request_many(requests))
            for response in responses:
                post_id = response.feature_values[feature_name].keys['post_id']
                count_1m = response.feature_values[feature_name].values['count_1m']
                if post_id in counts_by_post_id:
                    counts_by_post_id[post_id].add(count_1m)
                else:
                    counts_by_post_id[post_id] = {count_1m}

            if responses[0].feature_values[feature_name].values['count_1m'] == num_users:
                break
            time.sleep(push_period_s/2)
        for post_id in counts_by_post_id:
            assert num_users in counts_by_post_id[post_id]

        print('assert ok')


if __name__ == '__main__':
    t = TestPushPullE2E()
    t.test_counters()