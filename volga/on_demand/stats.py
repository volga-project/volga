from volga.stats.stats_manager import CounterConfig, HistogramConfig

ON_DEMAND_QPS_STATS_CONFIG = CounterConfig(
    name='on_demand_qps',
    aggregation_window_s=5
)

ON_DEMAND_SERVER_LATENCY_CONFIG = HistogramConfig(
    name='on_demand_server_latency',
    aggregation_window_s=5,
    percentiles=[99, 95, 75, 50],
    buckets=[*range(0, 10)] + [*range(10, 10000, 10)]
)

ON_DEMAND_DB_LATENCY_CONFIG = HistogramConfig(
    name='on_demand_db_latency',
    aggregation_window_s=5,
    percentiles=[99, 95, 75, 50],
    buckets=[*range(0, 10)] + [*range(10, 10000, 10)]
)