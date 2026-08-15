//! Runtime metrics: Prom fanout + in-process registry collect.

mod collect;
mod helpers;
mod histogram;
mod names;
mod registry;
mod types;

pub use collect::{
    collect_stream_task_metrics, collect_task_metric_values, get_stream_task_metrics, TaskMetricValues,
};
pub use helpers::{
    increment_pipeline_counter, increment_task_counter, record_pipeline_histogram,
    record_task_histogram, set_task_gauge,
};
pub use histogram::HistogramMetrics;
pub use names::*;
pub use registry::{
    init_metrics, install_metrics_http_from_env, prometheus_handle, DEFAULT_METRICS_PORT, METRICS_BIND_ENV,
};
pub use types::{
    OperatorAggregateMetrics, TaskMetrics, ThroughputMetrics, WorkerAggregateMetrics,
};
