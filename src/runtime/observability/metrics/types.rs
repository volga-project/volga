//! Snapshot / aggregate metric types.

use std::collections::HashMap;

use metrics::gauge;
use serde::{Deserialize, Serialize};

use crate::common::types::PipelineId;
use crate::runtime::execution_graph::ExecutionGraph;

use super::histogram::HistogramMetrics;
use super::names::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputMetrics {
    pub messages_sent: u64,
    pub messages_recv: u64,
    pub records_sent: u64,
    pub records_recv: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
}

impl ThroughputMetrics {
    pub fn new(messages_sent: u64, messages_recv: u64, records_sent: u64, records_recv: u64, bytes_sent: u64, bytes_recv: u64) -> Self {
        Self { messages_sent, messages_recv, records_sent, records_recv, bytes_sent, bytes_recv }
    }

    pub fn merge(stats: Vec<&ThroughputMetrics>) -> Self {
        let messages_sent = stats.iter().map(|s| s.messages_sent).sum();
        let messages_recv = stats.iter().map(|s| s.messages_recv).sum();
        let records_sent = stats.iter().map(|s| s.records_sent).sum();
        let records_recv = stats.iter().map(|s| s.records_recv).sum();
        let bytes_sent = stats.iter().map(|s| s.bytes_sent).sum();
        let bytes_recv = stats.iter().map(|s| s.bytes_recv).sum();
        
        Self::new(messages_sent, messages_recv, records_sent, records_recv, bytes_sent, bytes_recv)
    }
}

// per-task metrics (task/vertex scope)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub task_id: String,
    pub path_latency: HistogramMetrics,
    pub throughput_stast: ThroughputMetrics,
    pub backpressure_per_peer: HashMap<String, f64>,
}

impl TaskMetrics {
    pub fn empty(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            path_latency: HistogramMetrics::new(vec![0u64; LATENCY_HISTOGRAM_LEN]),
            throughput_stast: ThroughputMetrics::new(0, 0, 0, 0, 0, 0),
            backpressure_per_peer: HashMap::new(),
        }
    }
}

// aggregated metrics for an operator_id over all its tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorAggregateMetrics {
    pub operator_id: String,
    pub path_latency: HistogramMetrics,
    pub throughput_metrics: ThroughputMetrics,
}

impl OperatorAggregateMetrics {
    pub fn new(operator_id: String, task_metrics: Vec<TaskMetrics>) -> Self {
        let path_latency =
            HistogramMetrics::merge(task_metrics.iter().map(|m| &m.path_latency).collect());
        let throughput_stats =
            ThroughputMetrics::merge(task_metrics.iter().map(|m| &m.throughput_stast).collect());

        OperatorAggregateMetrics {
            operator_id,
            path_latency,
            throughput_metrics: throughput_stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAggregateMetrics {
    pub worker_id: String,
    pub pipeline_id: PipelineId,
    pub operator_metrics: HashMap<String, OperatorAggregateMetrics>,
    pub tasks_metrics: HashMap<String, TaskMetrics>
}

impl WorkerAggregateMetrics {

    pub fn new(worker_id: String, pipeline_id: PipelineId, tasks_metrics: HashMap<String, TaskMetrics>, graph: &ExecutionGraph) -> Self {
        let mut metrics_by_operator: HashMap<String, Vec<TaskMetrics>> = HashMap::new();

        // Group task metrics by operator_id
        for (vertex_id, task_metrics) in tasks_metrics.iter() {
            if let Some(vertex) = graph.get_vertex(vertex_id) {
                let operator_id = vertex.operator_id.clone();
                metrics_by_operator
                    .entry(operator_id)
                    .or_insert_with(Vec::new)
                    .push(task_metrics.clone());
            }
        }

        // Create OperatorMetrics for each operator by merging its task metrics
        let mut operator_metrics = HashMap::new();
        for (operator_id, task_metrics_vec) in metrics_by_operator.iter() {
            operator_metrics.insert(
                operator_id.clone(),
                OperatorAggregateMetrics::new(operator_id.clone(), task_metrics_vec.clone())
            );
        }

        WorkerAggregateMetrics {
            worker_id,
            pipeline_id,
            operator_metrics,
            tasks_metrics
        }
    }

    /// Publish all poll-time Prom write-backs for this worker snapshot.
    ///
    /// Includes operator/task rollups, max-backpressure gauges, and process RSS.
    /// Throughput rollups are gauges (set-to-current), not counters.
    pub fn publish_poll_gauges(&self, labels: &MetricsLabels) {
        self.publish_operator_and_latency_gauges();
        self.publish_backpressure_gauges();
        Self::publish_worker_memory_gauges(labels);
    }

    fn publish_operator_and_latency_gauges(&self) {
        let pipeline_id = self.pipeline_id.0.clone();

        for (operator_id, operator_metrics) in self.operator_metrics.iter() {
            let throughput = &operator_metrics.throughput_metrics;
            gauge!(
                METRIC_OPERATOR_MESSAGES_SENT,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.messages_sent as f64);
            gauge!(
                METRIC_OPERATOR_MESSAGES_RECV,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.messages_recv as f64);
            gauge!(
                METRIC_OPERATOR_RECORDS_SENT,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.records_sent as f64);
            gauge!(
                METRIC_OPERATOR_RECORDS_RECV,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.records_recv as f64);
            gauge!(
                METRIC_OPERATOR_BYTES_SENT,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.bytes_sent as f64);
            gauge!(
                METRIC_OPERATOR_BYTES_RECV,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(throughput.bytes_recv as f64);
            
            gauge!(
                METRIC_OPERATOR_PATH_LATENCY_99,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(operator_metrics.path_latency.p99);
            gauge!(
                METRIC_OPERATOR_PATH_LATENCY_95,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(operator_metrics.path_latency.p95);
            gauge!(
                METRIC_OPERATOR_PATH_LATENCY_50,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(operator_metrics.path_latency.p50);
            gauge!(
                METRIC_OPERATOR_PATH_LATENCY_AVG,
                LABEL_OPERATOR_ID => operator_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(operator_metrics.path_latency.avg);
        }

        for (vertex_id, task_metrics) in self.tasks_metrics.iter() {
            gauge!(
                METRIC_STREAM_TASK_PATH_LATENCY_99,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(task_metrics.path_latency.p99);
            gauge!(
                METRIC_STREAM_TASK_PATH_LATENCY_95,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(task_metrics.path_latency.p95);
            gauge!(
                METRIC_STREAM_TASK_PATH_LATENCY_50,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(task_metrics.path_latency.p50);
            gauge!(
                METRIC_STREAM_TASK_PATH_LATENCY_AVG,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            ).set(task_metrics.path_latency.avg);
        }
    }

    fn publish_backpressure_gauges(&self) {
        let pipeline_id = self.pipeline_id.0.clone();
        let mut worker_max_backpressure = 0.0f64;
        for (task_id, task_metrics) in self.tasks_metrics.iter() {
            let task_max = task_metrics
                .backpressure_per_peer
                .values()
                .copied()
                .fold(0.0f64, f64::max);
            worker_max_backpressure = worker_max_backpressure.max(task_max);
            gauge!(
                METRIC_STREAM_TASK_BACKPRESSURE_MAX,
                LABEL_TASK_ID => task_id.clone(),
                LABEL_WORKER_ID => self.worker_id.clone(),
                LABEL_PIPELINE_ID => pipeline_id.clone(),
            )
            .set(task_max);
        }
        gauge!(
            METRIC_WORKER_BACKPRESSURE_MAX,
            LABEL_WORKER_ID => self.worker_id.clone(),
            LABEL_PIPELINE_ID => pipeline_id,
        )
        .set(worker_max_backpressure);
    }

    fn publish_worker_memory_gauges(labels: &MetricsLabels) {
        let Some(usage) = memory_stats::memory_stats() else {
            return;
        };
        gauge!(
            METRIC_WORKER_RSS_BYTES,
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(usage.physical_mem as f64);
        gauge!(
            METRIC_WORKER_VIRTUAL_BYTES,
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(usage.virtual_mem as f64);
    }
}

