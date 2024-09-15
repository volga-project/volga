import collections
import copy
import time
import typing
from threading import Thread
from typing import Tuple, List, Dict

import numpy as np
import ray
from pydantic import BaseModel
from volga.streaming.common.utils import ms_to_s
from volga.streaming.runtime.master.stats.hist import Hist

REFRESH_PERIOD_S = 1
AGGREGATION_WINDOW_S = 5
LATENCY_BINS_MS = [*range(10, 100, 10)] + [*range(100, 1000, 100)] + [*range(1000, 11000, 1000)]

class _LatencyStats(BaseModel):
    latency_hists: typing.OrderedDict


class _ThroughputStats(BaseModel):
    num_messages: typing.OrderedDict


class WorkerStatsUpdate(BaseModel):
    pass


class WorkerLatencyStatsUpdate(WorkerStatsUpdate, _LatencyStats):
    pass


class WorkerThroughputStatsUpdate(WorkerStatsUpdate, _ThroughputStats):
    pass


class WorkerLatencyStatsState(_LatencyStats):

    @classmethod
    def init(cls) -> 'WorkerLatencyStatsState':
        return WorkerLatencyStatsState(latency_hists=collections.OrderedDict())

    def observe(self, latency_ms: int, ts_ms: int):
        s = ms_to_s(ts_ms)
        if s in self.latency_hists:
            hist = self.latency_hists[s]
            hist.observe(latency_ms)
        else:
            hist = Hist(LATENCY_BINS_MS)
            hist.observe(latency_ms)
            self.latency_hists[s] = hist

    def collect(self) -> WorkerLatencyStatsUpdate:
        c = copy.deepcopy(self.latency_hists)
        self.latency_hists = collections.OrderedDict()
        return WorkerLatencyStatsUpdate(latency_hists=c)


class WorkerThroughputStatsState(_ThroughputStats):

    @classmethod
    def init(cls) -> 'WorkerThroughputStatsState':
        return WorkerThroughputStatsState(num_messages=[])

    def inc(self):
        pass

    def collect(self) -> WorkerThroughputStatsUpdate:
        pass


class JobLatencyStatsState(_LatencyStats):

    aggregated: List[Tuple[float, Dict[str, int]]]

    def aggregate_updates(self, latency_updates: List[WorkerLatencyStatsUpdate]):
        # merge histograms for the same second
        grouped_hists = {}
        for lu in latency_updates:
            for s in lu.latency_hists:
                if s in grouped_hists:
                    grouped_hists[s].append(lu.latency_hists[s])
                else:
                    grouped_hists[s] = [lu.latency_hists[s]]
        merged_hists = {}
        for s in grouped_hists:
            merged_hists[s] = Hist.merge(grouped_hists[s])

        # update window
        for s in merged_hists:
            if s in self.latency_hists:
                self.latency_hists[s] = Hist.merge([self.latency_hists[s], merged_hists[s]])
            else:
                self.latency_hists[s] = merged_hists[s]

        # remove hists out of window
        secs = list(self.latency_hists.keys())
        for s in secs:
            if secs[-1] - s > AGGREGATION_WINDOW_S:
                del self.latency_hists[s]

        # calculate aggregates over window
        merged_window_hist = Hist.merge(list(self.latency_hists.values()))

        percentiles = [99, 95, 50]
        aggregates = merged_window_hist.percentiles(percentiles)
        d = {f'p{percentiles[i]}': aggregates[i] for i in range(len(percentiles))}
        self.aggregated.append((secs[-1], d))
        print(d)


class JobThroughputStatsState(_ThroughputStats):

    def aggregate_updates(self, throughput_updates: List[WorkerThroughputStatsUpdate]):
        pass


class StatsManager:

    def __init__(self):
        self._workers = []
        self._stats_collector_thread = Thread(target=self._collect_loop)
        self.running = False
        self.job_latency_stats = JobLatencyStatsState(latency_hists=[], aggregated=[])
        self.job_throughput_stats = JobThroughputStatsState(num_messages=[])

    def register_worker(self, worker):
        self._workers.append(worker)

    def _collect_stats_updates(self):
        futs = [w.collect_stats() for w in self._workers]
        res = ray.get(futs)
        latency_updates = []
        throughput_updates = []
        for r in res:
            if isinstance(r, WorkerLatencyStatsUpdate):
                latency_updates.append(r)
            elif isinstance(r, WorkerThroughputStatsUpdate):
                # throughput_updates.append(r)
                pass
            else:
                raise RuntimeError(f'Unknown stats update type {r.__class__.__name__}')

        self.job_latency_stats.aggregate_updates(latency_updates)
        self.job_throughput_stats.aggregate_updates(throughput_updates)

    def start(self):
        self.running = True
        self._stats_collector_thread.start()

    def _collect_loop(self):
        while self.running:
            self._collect_stats_updates()
            time.sleep(REFRESH_PERIOD_S)

    def stop(self):
        self.running = False
        self._stats_collector_thread.join(5)
        self._collect_stats_updates()


    def get_final_stats(self) -> Dict:
        raise NotImplementedError()
