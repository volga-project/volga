from sortedcontainers import SortedDict
import copy
import threading
import time
import typing
from threading import Thread
from typing import Tuple, List, Dict

import ray
from pydantic import BaseModel
from volga.streaming.common.utils import ms_to_s, now_ts_ms
from volga.stats.hist import Hist


class HistogramConfig(BaseModel):
    name: str
    aggregation_window_s: int
    percentiles: List[int]
    buckets: List[float]


class CounterConfig(BaseModel):
    name: str
    aggregation_window_s: int


class HistogramStatsBase(BaseModel):
    hists_per_s: SortedDict
    name: str


class CounterStatsBase(BaseModel):
    counts_per_s: SortedDict
    name: str


class StatsUpdate:
    pass


class HistogramStatsUpdate(StatsUpdate, HistogramStatsBase):
    pass


class CounterStatsUpdate(StatsUpdate, CounterStatsBase):
    pass


class HistogramStats(HistogramStatsBase):

    lock: typing.Any
    buckets: List[float]

    @classmethod
    def create(cls, config: HistogramConfig) -> 'HistogramStats':
        return HistogramStats(hists_per_s=SortedDict(), name=config.name, lock=threading.Lock(), buckets=config.buckets)

    def observe(self, value: int, ts_ms: int):
        self.lock.acquire()
        s = ms_to_s(ts_ms)
        if s in self.hists_per_s:
            hist = self.hists_per_s[s]
            hist.observe(value)
        else:
            hist = Hist(self.buckets)
            hist.observe(value)
            self.hists_per_s[s] = hist
        self.lock.release()

    def collect(self) -> HistogramStatsUpdate:
        self.lock.acquire()
        c = copy.deepcopy(self.hists_per_s)
        self.hists_per_s = SortedDict()
        self.lock.release()
        return HistogramStatsUpdate(name=self.name, hists_per_s=c)


class CounterStats(CounterStatsBase):

    lock: typing.Any
    cur_bucket_id: int
    cur_bucket_val: int

    @classmethod
    def create(cls, config: CounterConfig) -> 'CounterStats':
        return CounterStats(name=config.name, counts_per_s=SortedDict(), lock=threading.Lock(), cur_bucket_id=-1, cur_bucket_val=0)

    def inc(self, count: int = 1):
        s = ms_to_s(now_ts_ms())
        if self.cur_bucket_id < 0:
            self.cur_bucket_id = s
        if self.cur_bucket_id == s:
            self.cur_bucket_val += count
        else:
            self.lock.acquire()
            self.counts_per_s[self.cur_bucket_id] = self.cur_bucket_val
            self.lock.release()
            self.cur_bucket_id = s
            self.cur_bucket_val = count

    def collect(self) -> CounterStatsUpdate:
        self.lock.acquire()
        c = copy.deepcopy(self.counts_per_s)
        self.counts_per_s = SortedDict()
        self.lock.release()
        return CounterStatsUpdate(name=self.name, counts_per_s=c)


class HistoricalHistogramStats(HistogramStatsBase):

    historical_windowed_histogram_stats: List[Tuple[float, Dict[str, int]]]
    aggregation_window_s: int
    percentiles: List[int]

    def aggregate_updates(self, histogram_updates: List[HistogramStatsUpdate]):
        if len(histogram_updates) == 0:
            return
        # merge histograms for the same second
        grouped_hists = {}
        for lu in histogram_updates:
            for s in lu.hists_per_s:
                if s in grouped_hists:
                    grouped_hists[s].append(lu.hists_per_s[s])
                else:
                    grouped_hists[s] = [lu.hists_per_s[s]]
        merged_hists = {}
        for s in grouped_hists:
            merged_hists[s] = Hist.merge(grouped_hists[s])

        # update window
        for s in merged_hists:
            if s in self.hists_per_s:
                self.hists_per_s[s] = Hist.merge([self.hists_per_s[s], merged_hists[s]])
            else:
                self.hists_per_s[s] = merged_hists[s]

        if len(self.hists_per_s) == 0:
            return

        # create window
        secs = list(self.hists_per_s.keys())
        window = []
        for s in secs:
            if secs[-1] - s <= self.aggregation_window_s:
                window.append(self.hists_per_s[s])

        # calculate aggregates over window
        merged_window_hist = Hist.merge(window)

        aggregates = merged_window_hist.percentiles(self.percentiles)
        avg = merged_window_hist.avg()
        d = {f'p{self.percentiles[i]}': aggregates[i] for i in range(len(self.percentiles))}
        d['avg'] = avg
        self.historical_windowed_histogram_stats.append((secs[-1], d))
        print(f'[{secs[-1]}] {self.name}: {d}') # TODO proper logging


class HistoricalCounterStats(CounterStatsBase):

    historical_counts: List[Tuple[float, float]]
    aggregation_window_s: int

    def aggregate_updates(self, counter_updates: List[CounterStatsUpdate]):
        merged_counts = SortedDict()
        for up in counter_updates:
            for s in up.counts_per_s:
                if s in merged_counts:
                    merged_counts[s] += up.counts_per_s[s]
                else:
                    merged_counts[s] = up.counts_per_s[s]

        # update window
        for s in merged_counts:
            if s in self.counts_per_s:
                self.counts_per_s[s] += merged_counts[s]
            else:
                self.counts_per_s[s] = merged_counts[s]

        # remove old
        secs = list(self.counts_per_s.keys())
        if len(secs) == 0:
            return

        assert sorted(secs) == secs

        for s in secs:
            if secs[-1] - s >= self.aggregation_window_s:
                del self.counts_per_s[s]

        assert len(self.counts_per_s) <= self.aggregation_window_s

        # calculate aggregated rate over window
        agg = sum(list(self.counts_per_s.values()))/self.aggregation_window_s

        self.historical_counts.append((secs[-1], agg))
        print(f'[{secs[-1]}] {self.name}: {agg} count/s') # TODO proper logging


class HistoricalStats(BaseModel):
    histograms: Dict[str, HistoricalHistogramStats]
    counters: Dict[str, HistoricalCounterStats]


class StatsManager:

    def __init__(self, histograms: List[HistogramConfig], counters: List[CounterConfig], collect_period_s: int = 1):
        self._workers = []
        self._stats_collector_thread = Thread(target=self._collect_loop)
        self.running = False
        self.historical_stats = HistoricalStats(
            histograms={
                config.name: HistoricalHistogramStats(
                    name=config.name,
                    hists_per_s=SortedDict(),
                    historical_windowed_histogram_stats=[],
                    aggregation_window_s=config.aggregation_window_s,
                    percentiles=config.percentiles
                ) for config in histograms
            },
            counters={
                config.name: HistoricalCounterStats(
                    name=config.name,
                    counts_per_s=SortedDict(),
                    historical_counts=[],
                    aggregation_window_s=config.aggregation_window_s,
                ) for config in counters
            }
        )

        self.collect_period_s = collect_period_s
        for config in histograms:
            assert config.aggregation_window_s >= self.collect_period_s

        for config in counters:
            assert config.aggregation_window_s >= self.collect_period_s

    def register_worker(self, worker): # TODO make interface for stats-collectable worker
        self._workers.append(worker)

    def _collect_stats_updates(self):
        futs = [w.collect_stats.remote() for w in self._workers]
        res = ray.get(futs)
        histogram_updates = {}
        counter_updates = {}
        for updates in res:
            for update in updates:
                name = update.name
                if isinstance(update, HistogramStatsUpdate):
                    if name in histogram_updates:
                        histogram_updates[name].append(update)
                    else:
                        histogram_updates[name] = [update]
                elif isinstance(update, CounterStatsUpdate):
                    if name in counter_updates:
                        counter_updates[name].append(update)
                    else:
                        counter_updates[name] = [update]
                else:
                    raise RuntimeError(f'Unknown stats update type {update.__class__.__name__}')

        for name in histogram_updates:
            self.historical_stats.histograms[name].aggregate_updates(histogram_updates[name])

        for name in counter_updates:
            self.historical_stats.counters[name].aggregate_updates(counter_updates[name])

    def start(self):
        self.running = True
        self._stats_collector_thread.start()

    def _collect_loop(self):
        while self.running:
            self._collect_stats_updates()
            time.sleep(self.collect_period_s)

    def stop(self):
        self.running = False
        self._stats_collector_thread.join(5)
        self._collect_stats_updates()

    def get_historical_stats(self) -> HistoricalStats:
        return self.historical_stats
