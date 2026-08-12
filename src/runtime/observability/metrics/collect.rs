//! Collect task metrics / named values from the in-process registry.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::runtime::VertexId;

use super::names::*;
use super::registry::METRICS_REGISTRY;
use super::histogram::HistogramMetrics;
use super::types::{TaskMetrics, ThroughputMetrics};

fn key_label<'a>(key: &'a metrics::Key, name: &str) -> Option<&'a str> {
    key.labels().find(|l| l.key() == name).map(|l| l.value())
}

fn key_matches_labels(key: &metrics::Key, labels: Option<&MetricsLabels>) -> bool {
    let Some(labels) = labels else { return true; };
    key_label(key, LABEL_PIPELINE_ID) == Some(labels.pipeline_id.as_str())
        && key_label(key, LABEL_WORKER_ID) == Some(labels.worker_id.as_str())
}

#[derive(Default)]
struct TaskMetricsAcc {
    messages_sent: u64,
    messages_recv: u64,
    records_sent: u64,
    records_recv: u64,
    bytes_sent: u64,
    bytes_recv: u64,
    path_latency_histogram: Option<Vec<u64>>,
    backpressure_per_peer: HashMap<String, f64>,
}

pub fn collect_stream_task_metrics(labels: Option<&MetricsLabels>) -> HashMap<String, TaskMetrics> {
    let Some(registry) = METRICS_REGISTRY.get() else { return HashMap::new(); };
    let mut by_task: HashMap<String, TaskMetricsAcc> = HashMap::new();

    registry.visit_counters(|key, counter| {
        if !key_matches_labels(key, labels) { return; }
        let Some(task_id) = key_label(key, LABEL_TASK_ID) else { return; };
        let acc = by_task.entry(task_id.to_string()).or_default();
        let value = counter.load(Ordering::Relaxed);
        match key.name() {
            METRIC_STREAM_TASK_MESSAGES_SENT => acc.messages_sent = value,
            METRIC_STREAM_TASK_MESSAGES_RECV => acc.messages_recv = value,
            METRIC_STREAM_TASK_RECORDS_SENT => acc.records_sent = value,
            METRIC_STREAM_TASK_RECORDS_RECV => acc.records_recv = value,
            METRIC_STREAM_TASK_BYTES_SENT => acc.bytes_sent = value,
            METRIC_STREAM_TASK_BYTES_RECV => acc.bytes_recv = value,
            _ => {}
        }
    });

    registry.visit_gauges(|key, gauge| {
        if !key_matches_labels(key, labels) { return; }
        let Some(task_id) = key_label(key, LABEL_TASK_ID) else { return; };
        if key.name() != METRIC_STREAM_TASK_BACKPRESSURE_RATIO { return; }
        let Some(peer) = key_label(key, LABEL_TARGET_TASK_ID) else { return; };
        let value = f64::from_bits(gauge.load(Ordering::Relaxed));
        by_task.entry(task_id.to_string()).or_default()
            .backpressure_per_peer.insert(peer.to_string(), value);
    });

    registry.visit_histograms(|key, histogram| {
        if !key_matches_labels(key, labels) { return; }
        let Some(task_id) = key_label(key, LABEL_TASK_ID) else { return; };
        if key.name() != METRIC_STREAM_TASK_PATH_LATENCY { return; }
        by_task.entry(task_id.to_string()).or_default()
            .path_latency_histogram = Some(histogram.snapshot_buckets());
    });

    by_task.into_iter().map(|(task_id, acc)| {
        (task_id.clone(), TaskMetrics {
            task_id,
            path_latency: HistogramMetrics::new(
                acc.path_latency_histogram.unwrap_or_else(|| vec![0u64; LATENCY_HISTOGRAM_LEN]),
            ),
            throughput_stast: ThroughputMetrics::new(
                acc.messages_sent, acc.messages_recv, acc.records_sent, acc.records_recv,
                acc.bytes_sent, acc.bytes_recv,
            ),
            backpressure_per_peer: acc.backpressure_per_peer,
        })
    }).collect()
}

pub fn get_stream_task_metrics(task_id: VertexId, labels: Option<&MetricsLabels>) -> TaskMetrics {
    let id = task_id.as_ref();
    collect_stream_task_metrics(labels).remove(id).unwrap_or_else(|| TaskMetrics::empty(id))
}

#[derive(Debug, Default, Clone)]
pub struct TaskMetricValues {
    pub counters: HashMap<&'static str, u64>,
    pub gauges: HashMap<&'static str, f64>,
}

pub fn collect_task_metric_values(
    task_id: &str,
    labels: Option<&MetricsLabels>,
    names: &[&'static str],
) -> TaskMetricValues {
    let Some(registry) = METRICS_REGISTRY.get() else { return TaskMetricValues::default(); };
    let want: HashMap<&str, &'static str> = names.iter().map(|n| (*n, *n)).collect();
    let mut out = TaskMetricValues::default();
    registry.visit_counters(|key, counter| {
        if !key_matches_labels(key, labels) { return; }
        if key_label(key, LABEL_TASK_ID) != Some(task_id) { return; }
        if let Some(&name) = want.get(key.name()) {
            out.counters.insert(name, counter.load(Ordering::Relaxed));
        }
    });
    registry.visit_gauges(|key, gauge| {
        if !key_matches_labels(key, labels) { return; }
        if key_label(key, LABEL_TASK_ID) != Some(task_id) { return; }
        if let Some(&name) = want.get(key.name()) {
            out.gauges.insert(name, f64::from_bits(gauge.load(Ordering::Relaxed)));
        }
    });
    out
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metrics::{counter, histogram};

    use super::*;
    use crate::runtime::metrics::init_metrics;

    fn expected_cumulative(latencies: &[f64]) -> Vec<u64> {
        let mut histogram = vec![0u64; LATENCY_HISTOGRAM_LEN];
        for &latency in latencies {
            let mut idx = LATENCY_BUCKET_BOUNDARIES.len();
            for (i, &boundary) in LATENCY_BUCKET_BOUNDARIES.iter().enumerate() {
                if latency <= boundary {
                    idx = i;
                    break;
                }
            }
            for j in idx..histogram.len() {
                histogram[j] += 1;
            }
        }
        histogram
    }

    #[test]
    fn registry_collect_counters_hist_and_task_isolation() {
        init_metrics();

        let task_a = "test_task_a".to_string();
        let task_b = "test_task_b".to_string();
        let labels = MetricsLabels {
            pipeline_id: "pipe".to_string(),
            worker_id: "worker".to_string(),
        };

        counter!(
            METRIC_STREAM_TASK_MESSAGES_SENT,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(5);
        counter!(
            METRIC_STREAM_TASK_MESSAGES_RECV,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(3);
        counter!(
            METRIC_STREAM_TASK_RECORDS_SENT,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(25);
        counter!(
            METRIC_STREAM_TASK_RECORDS_RECV,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(15);
        counter!(
            METRIC_STREAM_TASK_BYTES_SENT,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(1024);
        counter!(
            METRIC_STREAM_TASK_BYTES_RECV,
            LABEL_TASK_ID => task_a.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        ).increment(512);

        let path = [5.0, 25.0, 150.0, 2.5, 75.0];
        for &latency in &path {
            histogram!(
                METRIC_STREAM_TASK_PATH_LATENCY,
                LABEL_TASK_ID => task_a.clone(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .record(latency);
        }

        counter!(
            METRIC_STREAM_TASK_MESSAGES_SENT,
            LABEL_TASK_ID => task_b.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone()
        )
        .increment(1);

        let collected = get_stream_task_metrics(Arc::<str>::from(task_a.clone()), Some(&labels));
        assert_eq!(collected.task_id, task_a);
        assert_eq!(collected.throughput_stast.messages_sent, 5);
        assert_eq!(collected.throughput_stast.messages_recv, 3);
        assert_eq!(collected.throughput_stast.records_sent, 25);
        assert_eq!(collected.throughput_stast.records_recv, 15);
        assert_eq!(collected.throughput_stast.bytes_sent, 1024);
        assert_eq!(collected.throughput_stast.bytes_recv, 512);
        assert_eq!(collected.path_latency.buckets, expected_cumulative(&path));

        let all = collect_stream_task_metrics(Some(&labels));
        assert_eq!(all.get(&task_a).unwrap().throughput_stast.messages_sent, 5);
        assert_eq!(all.get(&task_b).unwrap().throughput_stast.messages_sent, 1);
        assert_eq!(all.get(&task_b).unwrap().throughput_stast.messages_recv, 0);
    }
}
