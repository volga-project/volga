import collections
import copy
import threading
import time
import typing
from threading import Thread
from typing import Tuple, List, Dict

import ray
from pydantic import BaseModel
from volga.streaming.common.utils import ms_to_s
from volga.streaming.runtime.master.stats.hist import Hist

REFRESH_PERIOD_S = 1

LATENCY_AGGREGATION_WINDOW_S = 5

assert LATENCY_AGGREGATION_WINDOW_S >= REFRESH_PERIOD_S

LATENCY_BINS_MS = [*range(10, 10000, 10)]


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

    lock: typing.Any

    @classmethod
    def init(cls) -> 'WorkerLatencyStatsState':
        state = WorkerLatencyStatsState(latency_hists=collections.OrderedDict(), lock=threading.Lock())
        return state

    def observe(self, latency_ms: int, ts_ms: int):
        self.lock.acquire()
        s = ms_to_s(ts_ms)
        if s in self.latency_hists:
            hist = self.latency_hists[s]
            hist.observe(latency_ms)
        else:
            hist = Hist(LATENCY_BINS_MS)
            hist.observe(latency_ms)
            self.latency_hists[s] = hist
        self.lock.release()

    def collect(self) -> WorkerLatencyStatsUpdate:
        self.lock.acquire()
        c = copy.deepcopy(self.latency_hists)
        self.latency_hists = collections.OrderedDict()
        self.lock.release()
        return WorkerLatencyStatsUpdate(latency_hists=c)


class WorkerThroughputStatsState(_ThroughputStats):

    @classmethod
    def create(cls) -> 'WorkerThroughputStatsState':
        return WorkerThroughputStatsState(num_messages=collections.OrderedDict())

    def inc(self):
        pass

    def collect(self) -> WorkerThroughputStatsUpdate:
        pass


class JobLatencyStatsState(_LatencyStats):

    aggregated: List[Tuple[float, Dict[str, int]]]

    def aggregate_updates(self, latency_updates: List[WorkerLatencyStatsUpdate]):
        if len(latency_updates) == 0:
            return
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

        if len(self.latency_hists) == 0:
            return

        # remove hists out of window
        secs = list(self.latency_hists.keys())
        for s in secs:
            if secs[-1] - s > LATENCY_AGGREGATION_WINDOW_S:
                del self.latency_hists[s]

        # calculate aggregates over window
        merged_window_hist = Hist.merge(list(self.latency_hists.values()))

        percentiles = [95, 75, 50]
        aggregates = merged_window_hist.percentiles(percentiles)
        avg = merged_window_hist.avg()
        d = {f'p{percentiles[i]}': aggregates[i] for i in range(len(percentiles))}
        self.aggregated.append((secs[-1], d))
        print(d, avg)


class JobThroughputStatsState(_ThroughputStats):

    def aggregate_updates(self, throughput_updates: List[WorkerThroughputStatsUpdate]):
        pass


class StatsManager:

    def __init__(self):
        self._workers = []
        self._stats_collector_thread = Thread(target=self._collect_loop)
        self.running = False
        self.job_latency_stats = JobLatencyStatsState(latency_hists=collections.OrderedDict(), aggregated=[])
        self.job_throughput_stats = JobThroughputStatsState(num_messages=collections.OrderedDict())

    def register_worker(self, worker):
        self._workers.append(worker)

    def _collect_stats_updates(self):
        futs = [w.collect_stats.remote() for w in self._workers]
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
