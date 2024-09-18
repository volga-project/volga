import collections
import copy
import threading
import time
import typing
from threading import Thread
from typing import Tuple, List, Dict

import ray
from pydantic import BaseModel
from volga.streaming.common.utils import ms_to_s, now_ts_ms
from volga.streaming.runtime.master.stats.hist import Hist

REFRESH_PERIOD_S = 1

LATENCY_BINS_MS = [*range(10, 10000, 10)]

LATENCY_AGGREGATION_WINDOW_S = 5

assert LATENCY_AGGREGATION_WINDOW_S >= REFRESH_PERIOD_S

THROUGHPUT_AGGREGATION_WINDOW_S = 2

assert THROUGHPUT_AGGREGATION_WINDOW_S >= REFRESH_PERIOD_S



class _LatencyStats(BaseModel):
    latency_hists_per_s: typing.OrderedDict


class _ThroughputStats(BaseModel):
    num_messages_per_s: typing.OrderedDict


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
        state = WorkerLatencyStatsState(latency_hists_per_s=collections.OrderedDict(), lock=threading.Lock())
        return state

    def observe(self, latency_ms: int, ts_ms: int):
        self.lock.acquire()
        s = ms_to_s(ts_ms)
        if s in self.latency_hists_per_s:
            hist = self.latency_hists_per_s[s]
            hist.observe(latency_ms)
        else:
            hist = Hist(LATENCY_BINS_MS)
            hist.observe(latency_ms)
            self.latency_hists_per_s[s] = hist
        self.lock.release()

    def collect(self) -> WorkerLatencyStatsUpdate:
        self.lock.acquire()
        c = copy.deepcopy(self.latency_hists_per_s)
        self.latency_hists_per_s = collections.OrderedDict()
        self.lock.release()
        return WorkerLatencyStatsUpdate(latency_hists_per_s=c)


class WorkerThroughputStatsState(_ThroughputStats):

    lock: typing.Any
    cur_bucket_id: int
    cur_bucket_val: int

    @classmethod
    def create(cls) -> 'WorkerThroughputStatsState':
        return WorkerThroughputStatsState(num_messages_per_s=collections.OrderedDict(), lock=threading.Lock(), cur_bucket_id=-1, cur_bucket_val=0)

    def inc(self):
        s = ms_to_s(now_ts_ms())
        if self.cur_bucket_id < 0:
            self.cur_bucket_id = s
        if self.cur_bucket_id == s:
            self.cur_bucket_val += 1
        else:
            self.lock.acquire()
            self.num_messages_per_s[self.cur_bucket_id] = self.cur_bucket_val
            self.lock.release()
            self.cur_bucket_id = s
            self.cur_bucket_val = 1

    def collect(self) -> WorkerThroughputStatsUpdate:
        self.lock.acquire()
        c = copy.deepcopy(self.num_messages_per_s)
        self.num_messages_per_s = collections.OrderedDict()
        self.lock.release()
        return WorkerThroughputStatsUpdate(num_messages_per_s=c)


class JobLatencyStatsState(_LatencyStats):

    aggregated_latency_stats: List[Tuple[float, Dict[str, int]]]

    def aggregate_updates(self, latency_updates: List[WorkerLatencyStatsUpdate]):
        if len(latency_updates) == 0:
            return
        # merge histograms for the same second
        grouped_hists = {}
        for lu in latency_updates:
            for s in lu.latency_hists_per_s:
                if s in grouped_hists:
                    grouped_hists[s].append(lu.latency_hists_per_s[s])
                else:
                    grouped_hists[s] = [lu.latency_hists_per_s[s]]
        merged_hists = {}
        for s in grouped_hists:
            merged_hists[s] = Hist.merge(grouped_hists[s])

        # update window
        for s in merged_hists:
            if s in self.latency_hists_per_s:
                self.latency_hists_per_s[s] = Hist.merge([self.latency_hists_per_s[s], merged_hists[s]])
            else:
                self.latency_hists_per_s[s] = merged_hists[s]

        if len(self.latency_hists_per_s) == 0:
            return

        # remove hists out of window
        secs = list(self.latency_hists_per_s.keys())
        for s in secs:
            if secs[-1] - s > LATENCY_AGGREGATION_WINDOW_S:
                del self.latency_hists_per_s[s]

        # calculate aggregates over window
        merged_window_hist = Hist.merge(list(self.latency_hists_per_s.values()))

        percentiles = [95, 75, 50]
        aggregates = merged_window_hist.percentiles(percentiles)
        avg = merged_window_hist.avg()
        d = {f'p{percentiles[i]}': aggregates[i] for i in range(len(percentiles))}
        d['avg'] = avg
        self.aggregated_latency_stats.append((secs[-1], d))
        print(f'Latency: {d}')


class JobThroughputStatsState(_ThroughputStats):

    aggregated_throughput: List[Tuple[float, Dict[str, int]]]

    def aggregate_updates(self, throughput_updates: List[WorkerThroughputStatsUpdate]):
        merged_num_messages = collections.OrderedDict()
        for up in throughput_updates:
            for s in up.num_messages_per_s:
                if s in merged_num_messages:
                    merged_num_messages[s] += up.num_messages_per_s[s]
                else:
                    merged_num_messages[s] = up.num_messages_per_s[s]

        # update window
        for s in merged_num_messages:
            if s in self.num_messages_per_s:
                self.num_messages_per_s[s] += merged_num_messages[s]
            else:
                self.num_messages_per_s[s] = merged_num_messages[s]

        # remove old
        secs = list(self.num_messages_per_s.keys())
        if len(secs) == 0:
            return

        for s in secs:
            if secs[-1] - s > THROUGHPUT_AGGREGATION_WINDOW_S:
                del self.num_messages_per_s[s]

        # calculate aggregated rate over window
        agg = sum(list(self.num_messages_per_s.values()))/THROUGHPUT_AGGREGATION_WINDOW_S

        self.aggregated_throughput.append((secs[-1], agg))

        print(f'Throughput: {agg} msg/s')


class StatsManager:

    def __init__(self):
        self._workers = []
        self._stats_collector_thread = Thread(target=self._collect_loop)
        self.running = False
        self.job_latency_stats = JobLatencyStatsState(latency_hists_per_s=collections.OrderedDict(), aggregated_latency_stats=[])
        self.job_throughput_stats = JobThroughputStatsState(num_messages_per_s=collections.OrderedDict(), aggregated_throughput=[])

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
                throughput_updates.append(r)
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
