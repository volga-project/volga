from typing import Tuple, Dict, List

from volga.stats.hist import Hist
from volga.stats.stats_manager import HistogramConfig, CounterConfig, StatsManager, HistoricalStats

LATENCY_STATS_CONFIG = HistogramConfig(
    name='streaming_latency',
    aggregation_window_s=5,
    percentiles=[99, 95, 75, 50],
    buckets=[*range(0, 10)] + [*range(10, 10000, 10)]
)

THROUGHPUT_STATS_CONFIG = CounterConfig(
    name='streaming_throughput',
    aggregation_window_s=5,
)


def create_streaming_stats_manager() -> StatsManager:
    return StatsManager(histograms=[LATENCY_STATS_CONFIG], counters=[THROUGHPUT_STATS_CONFIG])


# returns avg throughput + dict of p99,95,75,50 latencies aggregated over the whole run
def aggregate_streaming_historical_stats(
    historical_stats: HistoricalStats,
    warmup_thresh_s: int = 10 # disregard first seconds of hist data - those are warm-up outliers
) -> Tuple[float, Dict[str, float], List[Tuple[float, float]], List[Tuple[float, Dict[str, int]]]]:
    historical_latency = historical_stats.histograms[LATENCY_STATS_CONFIG.name]
    historical_latency_hists = list(historical_latency.hists_per_s.items())

    historical_throughput = historical_stats.counters[THROUGHPUT_STATS_CONFIG.name].historical_counts
    historical_throughput_values = list(map(lambda e: e[1], historical_throughput))
    if len(historical_throughput_values) - warmup_thresh_s < 10:
        # warning
        print(f'[WARNING] We expect at least {warmup_thresh_s + 10} seconds of historical throughput data, got {len(historical_throughput_values)}')

    historical_throughput_values = historical_throughput_values[warmup_thresh_s:]
    if len(historical_throughput_values) != 0:
        avg_throughput = sum(historical_throughput_values)/len(historical_throughput_values)
    else:
        avg_throughput = 0

    historical_latency_hists = list(map(lambda e: e[1], historical_latency_hists))
    if len(historical_latency_hists) - warmup_thresh_s < 10:
        # warning
        print(f'[WARNING] We expect at least {warmup_thresh_s + 10} seconds of historical latency data, got {len(historical_latency_hists)}')

    historical_latency_hists = historical_latency_hists[warmup_thresh_s:]
    merged = Hist.merge(historical_latency_hists)
    aggregates = merged.percentiles(LATENCY_STATS_CONFIG.percentiles)
    avg = merged.avg()
    latency_stats = {f'p{LATENCY_STATS_CONFIG.percentiles[i]}': aggregates[i] for i in range(len(LATENCY_STATS_CONFIG.percentiles))}
    latency_stats['avg'] = avg

    hist_latency = historical_stats.histograms[LATENCY_STATS_CONFIG.name].historical_windowed_histogram_stats

    return avg_throughput, latency_stats, historical_throughput, hist_latency
