use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_exporter_tcp::TcpBuilder;
use metrics_util::layers::FanoutBuilder;
use prometheus_parse::{Scrape, Value, HistogramCount};
use std::{collections::HashMap, sync::{Once, OnceLock}};

use crate::runtime::VertexId;
use crate::runtime::execution_graph::ExecutionGraph;

// Global Prometheus handle for programmatic access
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INIT_METRICS: Once = Once::new();

pub fn init_metrics() {
    INIT_METRICS.call_once(|| {
        _init_metrics().expect("Failed to initialize metrics");
    });
}

// Stream task metrics
pub const METRIC_STREAM_TASK_MESSAGES_SENT: &str = "volga_stream_task_messages_sent";
pub const METRIC_STREAM_TASK_MESSAGES_RECV: &str = "volga_stream_task_messages_recv";
pub const METRIC_STREAM_TASK_RECORDS_SENT: &str = "volga_stream_task_records_sent";
pub const METRIC_STREAM_TASK_RECORDS_RECV: &str = "volga_stream_task_records_recv";
pub const METRIC_STREAM_TASK_BYTES_SENT: &str = "volga_stream_task_bytes_sent";
pub const METRIC_STREAM_TASK_BYTES_RECV: &str = "volga_stream_task_bytes_recv";
pub const METRIC_STREAM_TASK_LATENCY: &str = "volga_stream_task_latency";
pub const METRIC_STREAM_TASK_LATENCY_99: &str = "volga_stream_task_latency_99";
pub const METRIC_STREAM_TASK_LATENCY_95: &str = "volga_stream_task_latency_95";
pub const METRIC_STREAM_TASK_LATENCY_50: &str = "volga_stream_task_latency_50";
pub const METRIC_STREAM_TASK_LATENCY_AVG: &str = "volga_stream_task_latency_avg";
pub const METRIC_STREAM_TASK_TX_QUEUE_SIZE: &str = "volga_stream_task_tx_queue_size";
pub const METRIC_STREAM_TASK_TX_QUEUE_REM: &str = "volga_stream_task_tx_queue_rem";
pub const METRIC_STREAM_TASK_BACKPRESSURE_RATIO: &str = "volga_stream_task_backpressure_ratio";

// Operator metrics
pub const METRIC_OPERATOR_MESSAGES_SENT: &str = "volga_operator_messages_sent";
pub const METRIC_OPERATOR_MESSAGES_RECV: &str = "volga_operator_messages_recv";
pub const METRIC_OPERATOR_RECORDS_SENT: &str = "volga_operator_records_sent";
pub const METRIC_OPERATOR_RECORDS_RECV: &str = "volga_operator_records_recv";
pub const METRIC_OPERATOR_BYTES_SENT: &str = "volga_operator_bytes_sent";
pub const METRIC_OPERATOR_BYTES_RECV: &str = "volga_operator_bytes_recv";
pub const METRIC_OPERATOR_LATENCY_99: &str = "volga_operator_latency_99";
pub const METRIC_OPERATOR_LATENCY_95: &str = "volga_operator_latency_95";
pub const METRIC_OPERATOR_LATENCY_50: &str = "volga_operator_latency_50";
pub const METRIC_OPERATOR_LATENCY_AVG: &str = "volga_operator_latency_avg";

// Label constants
pub const LABEL_VERTEX_ID: &str = "vertex_id";
pub const LABEL_TARGET_VERTEX_ID: &str = "target_vertex_id";
pub const LABEL_WORKER_ID: &str = "worker_id";
pub const LABEL_OPERATOR_ID: &str = "operator_id";

// Histogram bucket boundaries, milliseconds
pub const LATENCY_BUCKET_BOUNDARIES: [f64; 12] = [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetrics {
    pub latency_histogram: Vec<u64>, // latency is calculated from ingestion time to this task
    pub p99: f64,
    pub p95: f64,
    pub p50: f64,
    pub avg: f64,
}

impl LatencyMetrics {
    pub fn new(latency_histogram: Vec<u64>) -> Self {
        let (p99, p95, p50, avg) = Self::calculate_stats(&latency_histogram);
        Self {
            latency_histogram,
            p99,
            p95,
            p50,
            avg,
        }
    }

    pub fn merge(latency_stats: Vec<&LatencyMetrics>) -> Self {
        if latency_stats.is_empty() {
            return Self::new(vec![0u64; LATENCY_BUCKET_BOUNDARIES.len()]);
        }

        // Merge histograms by summing corresponding buckets
        let mut merged_histogram = vec![0u64; LATENCY_BUCKET_BOUNDARIES.len()];
        for stats in latency_stats.iter() {
            for (i, bucket) in merged_histogram.iter_mut().enumerate() {
                if i < stats.latency_histogram.len() {
                    *bucket += stats.latency_histogram[i];
                }
            }
        }

        Self::new(merged_histogram)
    }

    /// Calculate histogram statistics (p99, p95, p50, avg) from a histogram
    pub fn calculate_stats(histogram: &Vec<u64>) -> (f64, f64, f64, f64) {
        // Handle edge cases
        if histogram.is_empty() || histogram.len() != LATENCY_BUCKET_BOUNDARIES.len() {
            return (0.0, 0.0, 0.0, 0.0);
        }
        
        // Get total sample count from last bucket (cumulative histogram)
        let total_samples = *histogram.last().unwrap();
        if total_samples == 0 {
            return (0.0, 0.0, 0.0, 0.0);
        }
        
        // Calculate percentiles
        let p99 = Self::calculate_percentile(histogram, total_samples, 99.0);
        let p95 = Self::calculate_percentile(histogram, total_samples, 95.0);
        let p50 = Self::calculate_percentile(histogram, total_samples, 50.0);
        
        // Calculate average using weighted sum of bucket midpoints
        let avg = Self::calculate_weighted_average(histogram, total_samples);
        
        (p99, p95, p50, avg)
    }

    fn calculate_percentile(histogram: &Vec<u64>, total_samples: u64, percentile: f64) -> f64 {
        let boundaries = &LATENCY_BUCKET_BOUNDARIES;
        let target_count = (total_samples as f64 * percentile / 100.0) as u64;
        
        // Find the bucket containing the target count
        for (i, &bucket_count) in histogram.iter().enumerate() {
            if bucket_count >= target_count {
                let boundary = boundaries[i];
                
                // Linear interpolation within the bucket
                let prev_count = if i == 0 { 0 } else { histogram[i-1] };
                let bucket_samples = bucket_count - prev_count;
                
                if bucket_samples == 0 {
                    return boundary;
                }
                
                let prev_boundary = if i == 0 { 0.0 } else { boundaries[i-1] };
                let samples_into_bucket = target_count - prev_count;
                let fraction = samples_into_bucket as f64 / bucket_samples as f64;
                
                return prev_boundary + (boundary - prev_boundary) * fraction;
            }
        }
        
        // Fallback: return the last boundary
        *LATENCY_BUCKET_BOUNDARIES.last().unwrap()
    }

    fn calculate_weighted_average(histogram: &Vec<u64>, total_samples: u64) -> f64 {
        let boundaries = &LATENCY_BUCKET_BOUNDARIES;
        let mut weighted_sum = 0.0;
        
        for (i, &bucket_count) in histogram.iter().enumerate() {
            let boundary = boundaries[i];
            
            // Calculate bucket midpoint
            let prev_boundary = if i == 0 { 0.0 } else { boundaries[i-1] };
            let bucket_midpoint = (prev_boundary + boundary) / 2.0;
            
            // Get samples in this bucket (not cumulative)
            let prev_count = if i == 0 { 0 } else { histogram[i-1] };
            let bucket_samples = bucket_count - prev_count;
            
            weighted_sum += bucket_midpoint * bucket_samples as f64;
        }
        
        weighted_sum / total_samples as f64
    }
}

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

// per-task metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTaskMetrics {
    pub vertex_id: String,
    pub latency_stats: LatencyMetrics,
    pub throughput_stast: ThroughputMetrics,
    pub backpressure_per_peer: HashMap<String, f64>,
}

// aggregated metrics for an operator over all it's tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    pub operator_id: String,
    pub latency_metrics: LatencyMetrics,
    pub throughput_metrics: ThroughputMetrics,
}

impl OperatorMetrics {
    pub fn new(operator_id: String, task_metrics: Vec<StreamTaskMetrics>) -> Self {
        let latency_stats = LatencyMetrics::merge(task_metrics.iter().map(|m| &m.latency_stats).collect());
        let throughput_stats = ThroughputMetrics::merge(task_metrics.iter().map(|m| &m.throughput_stast).collect());

        OperatorMetrics {
            operator_id,
            latency_metrics: latency_stats,
            throughput_metrics: throughput_stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetrics {
    pub worker_id: String,
    pub operator_metrics: HashMap<String, OperatorMetrics>,
    pub tasks_metrics: HashMap<String, StreamTaskMetrics>
}

impl WorkerMetrics {

    pub fn new(worker_id: String, tasks_metrics: HashMap<String, StreamTaskMetrics>, graph: &ExecutionGraph) -> Self {
        let mut metrics_by_operator: HashMap<String, Vec<StreamTaskMetrics>> = HashMap::new();

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
                OperatorMetrics::new(operator_id.clone(), task_metrics_vec.clone())
            );
        }

        WorkerMetrics {
            worker_id,
            operator_metrics,
            tasks_metrics
        }
    }

    pub fn record(&self) {
        for (operator_id, operator_metrics) in self.operator_metrics.iter() {
            // Record throughput metrics
            counter!(METRIC_OPERATOR_MESSAGES_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.messages_sent);
            counter!(METRIC_OPERATOR_MESSAGES_RECV, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.messages_recv);
            counter!(METRIC_OPERATOR_RECORDS_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.records_sent);
            counter!(METRIC_OPERATOR_RECORDS_RECV, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.records_recv);
            counter!(METRIC_OPERATOR_BYTES_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.bytes_sent);
            counter!(METRIC_OPERATOR_BYTES_RECV, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.throughput_metrics.bytes_recv);
            
            // Record latency metrics
            gauge!(METRIC_OPERATOR_LATENCY_99, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).set(operator_metrics.latency_metrics.p99);
            gauge!(METRIC_OPERATOR_LATENCY_95, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).set(operator_metrics.latency_metrics.p95);
            gauge!(METRIC_OPERATOR_LATENCY_50, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).set(operator_metrics.latency_metrics.p50);
            gauge!(METRIC_OPERATOR_LATENCY_AVG, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).set(operator_metrics.latency_metrics.avg);
        }
        
        // Record stream task latency metrics
        for (vertex_id, task_metrics) in self.tasks_metrics.iter() {
            gauge!(METRIC_STREAM_TASK_LATENCY_99, LABEL_VERTEX_ID => vertex_id.clone()).set(task_metrics.latency_stats.p99);
            gauge!(METRIC_STREAM_TASK_LATENCY_95, LABEL_VERTEX_ID => vertex_id.clone()).set(task_metrics.latency_stats.p95);
            gauge!(METRIC_STREAM_TASK_LATENCY_50, LABEL_VERTEX_ID => vertex_id.clone()).set(task_metrics.latency_stats.p50);
            gauge!(METRIC_STREAM_TASK_LATENCY_AVG, LABEL_VERTEX_ID => vertex_id.clone()).set(task_metrics.latency_stats.avg);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineStateHistory {
    pub samples: Vec<(u64, crate::runtime::master::PipelineState)>,
}

#[derive(Debug, Clone)]
pub struct ThroughputStats {
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub stddev: f64,
}

#[derive(Debug, Clone)]
pub struct TaskThroughputStats {
    pub messages_sent: ThroughputStats,
    pub messages_recv: ThroughputStats,
    pub records_sent: ThroughputStats,
    pub records_recv: ThroughputStats,
    pub bytes_sent: ThroughputStats,
    pub bytes_recv: ThroughputStats,
}

#[derive(Debug, Clone)]
pub struct FinalStats {
    pub throughput_per_task: HashMap<String, TaskThroughputStats>,
    pub latency_per_task: HashMap<String, LatencyMetrics>,
}

impl PipelineStateHistory {
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, timestamp: u64, pipeline_state: crate::runtime::master::PipelineState) {
        self.samples.push((timestamp, pipeline_state));
    }

    pub fn calculate_throughput_rates(&self, window_seconds: u64) -> HashMap<String, ThroughputRates> {
        let mut rates = HashMap::new();
        
        if self.samples.is_empty() {
            return rates;
        }
        
        // Use the last sample as the end state
        let (end_timestamp, end_state) = self.samples.last().unwrap();
        let end_timestamp = *end_timestamp;
        
        let window_start_timestamp = end_timestamp.saturating_sub(window_seconds);
        
        // Find the first state within the window
        let window_start_idx = self.samples
            .iter()
            .position(|(ts, _)| *ts >= window_start_timestamp)
            .unwrap_or(0);
        
        if window_start_idx >= self.samples.len() {
            return rates;
        }
        
        let (start_timestamp, start_state) = &self.samples[window_start_idx];
        let start_timestamp = *start_timestamp;
        
        let time_diff_seconds = (end_timestamp - start_timestamp) as f64;
        if time_diff_seconds <= 0.0 {
            return rates;
        }
        
        // Calculate rates for each task (vertex)
        for (worker_id, end_worker_state) in &end_state.worker_states {
            if let Some(end_worker_metrics) = &end_worker_state.worker_metrics {
                if let Some(start_worker_state) = start_state.worker_states.get(worker_id) {
                    if let Some(start_worker_metrics) = &start_worker_state.worker_metrics {
                        for (vertex_id, end_task_metrics) in &end_worker_metrics.tasks_metrics {
                            if let Some(start_task_metrics) = start_worker_metrics.tasks_metrics.get(vertex_id) {
                                let messages_sent_diff = end_task_metrics.throughput_stast.messages_sent
                                    .saturating_sub(start_task_metrics.throughput_stast.messages_sent);
                                let messages_recv_diff = end_task_metrics.throughput_stast.messages_recv
                                    .saturating_sub(start_task_metrics.throughput_stast.messages_recv);
                                let records_sent_diff = end_task_metrics.throughput_stast.records_sent
                                    .saturating_sub(start_task_metrics.throughput_stast.records_sent);
                                let records_recv_diff = end_task_metrics.throughput_stast.records_recv
                                    .saturating_sub(start_task_metrics.throughput_stast.records_recv);
                                let bytes_sent_diff = end_task_metrics.throughput_stast.bytes_sent
                                    .saturating_sub(start_task_metrics.throughput_stast.bytes_sent);
                                let bytes_recv_diff = end_task_metrics.throughput_stast.bytes_recv
                                    .saturating_sub(start_task_metrics.throughput_stast.bytes_recv);
                                
                                rates.insert(vertex_id.clone(), ThroughputRates {
                                    messages_sent_per_sec: messages_sent_diff as f64 / time_diff_seconds,
                                    messages_recv_per_sec: messages_recv_diff as f64 / time_diff_seconds,
                                    records_sent_per_sec: records_sent_diff as f64 / time_diff_seconds,
                                    records_recv_per_sec: records_recv_diff as f64 / time_diff_seconds,
                                    bytes_sent_per_sec: bytes_sent_diff as f64 / time_diff_seconds,
                                    bytes_recv_per_sec: bytes_recv_diff as f64 / time_diff_seconds,
                                });
                            }
                        }
                    }
                }
            }
        }
        
        rates
    }

    pub fn final_stats(&self) -> FinalStats {
        let mut throughput_per_task: HashMap<String, TaskThroughputStats> = HashMap::new();
        let mut latency_per_task: HashMap<String, LatencyMetrics> = HashMap::new();
        
        if self.samples.len() < 2 {
            return FinalStats {
                throughput_per_task,
                latency_per_task,
            };
        }
        
        // Calculate throughput rates for each consecutive pair of samples
        let mut all_rates_per_task: HashMap<String, Vec<ThroughputRates>> = HashMap::new();
        
        for i in 1..self.samples.len() {
            let (start_timestamp, start_state) = &self.samples[i - 1];
            let (end_timestamp, end_state) = &self.samples[i];
            
            let time_diff_seconds = (*end_timestamp - *start_timestamp) as f64;
            if time_diff_seconds <= 0.0 {
                continue;
            }
            
            // Calculate rates for each task
            for (worker_id, end_worker_state) in &end_state.worker_states {
                if let Some(end_worker_metrics) = &end_worker_state.worker_metrics {
                    if let Some(start_worker_state) = start_state.worker_states.get(worker_id) {
                        if let Some(start_worker_metrics) = &start_worker_state.worker_metrics {
                            for (vertex_id, end_task_metrics) in &end_worker_metrics.tasks_metrics {
                                if let Some(start_task_metrics) = start_worker_metrics.tasks_metrics.get(vertex_id) {
                                    let messages_sent_diff = end_task_metrics.throughput_stast.messages_sent
                                        .saturating_sub(start_task_metrics.throughput_stast.messages_sent);
                                    let messages_recv_diff = end_task_metrics.throughput_stast.messages_recv
                                        .saturating_sub(start_task_metrics.throughput_stast.messages_recv);
                                    let records_sent_diff = end_task_metrics.throughput_stast.records_sent
                                        .saturating_sub(start_task_metrics.throughput_stast.records_sent);
                                    let records_recv_diff = end_task_metrics.throughput_stast.records_recv
                                        .saturating_sub(start_task_metrics.throughput_stast.records_recv);
                                    let bytes_sent_diff = end_task_metrics.throughput_stast.bytes_sent
                                        .saturating_sub(start_task_metrics.throughput_stast.bytes_sent);
                                    let bytes_recv_diff = end_task_metrics.throughput_stast.bytes_recv
                                        .saturating_sub(start_task_metrics.throughput_stast.bytes_recv);
                                    
                                    let rates = ThroughputRates {
                                        messages_sent_per_sec: messages_sent_diff as f64 / time_diff_seconds,
                                        messages_recv_per_sec: messages_recv_diff as f64 / time_diff_seconds,
                                        records_sent_per_sec: records_sent_diff as f64 / time_diff_seconds,
                                        records_recv_per_sec: records_recv_diff as f64 / time_diff_seconds,
                                        bytes_sent_per_sec: bytes_sent_diff as f64 / time_diff_seconds,
                                        bytes_recv_per_sec: bytes_recv_diff as f64 / time_diff_seconds,
                                    };
                                    
                                    all_rates_per_task.entry(vertex_id.clone())
                                        .or_insert_with(Vec::new)
                                        .push(rates);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Calculate stats (avg, min, max, stddev) for each task's throughput rates
        for (vertex_id, rates_vec) in all_rates_per_task {
            if rates_vec.is_empty() {
                continue;
            }
            
            let messages_sent_values: Vec<f64> = rates_vec.iter().map(|r| r.messages_sent_per_sec).collect();
            let messages_recv_values: Vec<f64> = rates_vec.iter().map(|r| r.messages_recv_per_sec).collect();
            let records_sent_values: Vec<f64> = rates_vec.iter().map(|r| r.records_sent_per_sec).collect();
            let records_recv_values: Vec<f64> = rates_vec.iter().map(|r| r.records_recv_per_sec).collect();
            let bytes_sent_values: Vec<f64> = rates_vec.iter().map(|r| r.bytes_sent_per_sec).collect();
            let bytes_recv_values: Vec<f64> = rates_vec.iter().map(|r| r.bytes_recv_per_sec).collect();
            
            throughput_per_task.insert(vertex_id.clone(), TaskThroughputStats {
                messages_sent: Self::calculate_stats(&messages_sent_values),
                messages_recv: Self::calculate_stats(&messages_recv_values),
                records_sent: Self::calculate_stats(&records_sent_values),
                records_recv: Self::calculate_stats(&records_recv_values),
                bytes_sent: Self::calculate_stats(&bytes_sent_values),
                bytes_recv: Self::calculate_stats(&bytes_recv_values),
            });
        }
        
        // Aggregate latency histograms per task across all samples
        let mut latency_histograms_per_task: HashMap<String, Vec<&LatencyMetrics>> = HashMap::new();
        
        for (_timestamp, pipeline_state) in &self.samples {
            for (_worker_id, worker_state) in &pipeline_state.worker_states {
                if let Some(worker_metrics) = &worker_state.worker_metrics {
                    for (vertex_id, task_metrics) in &worker_metrics.tasks_metrics {
                        latency_histograms_per_task.entry(vertex_id.clone())
                            .or_insert_with(Vec::new)
                            .push(&task_metrics.latency_stats);
                    }
                }
            }
        }
        
        // Merge histograms and calculate stats for each task
        for (vertex_id, latency_metrics_vec) in latency_histograms_per_task {
            let merged_latency = LatencyMetrics::merge(latency_metrics_vec);
            latency_per_task.insert(vertex_id, merged_latency);
        }
        
        FinalStats {
            throughput_per_task,
            latency_per_task,
        }
    }
    
    fn calculate_stats(values: &[f64]) -> ThroughputStats {
        if values.is_empty() {
            return ThroughputStats {
                avg: 0.0,
                min: 0.0,
                max: 0.0,
                stddev: 0.0,
            };
        }
        
        let avg = values.iter().sum::<f64>() / values.len() as f64;
        let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        let variance = if values.len() > 1 {
            values.iter()
                .map(|x| (x - avg).powi(2))
                .sum::<f64>() / (values.len() - 1) as f64
        } else {
            0.0
        };
        let stddev = variance.sqrt();
        
        ThroughputStats {
            avg,
            min,
            max,
            stddev,
        }
    }
}

pub struct ThroughputRates {
    pub messages_sent_per_sec: f64,
    pub messages_recv_per_sec: f64,
    pub records_sent_per_sec: f64,
    pub records_recv_per_sec: f64,
    pub bytes_sent_per_sec: f64,
    pub bytes_recv_per_sec: f64,
}

fn _init_metrics() -> Result<(), Box<dyn std::error::Error>> {
    // Assert that we don't have f64::MAX in bucket boundaries
    assert!(!LATENCY_BUCKET_BOUNDARIES.contains(&f64::MAX), 
            "LATENCY_BUCKET_BOUNDARIES should not contain f64::MAX");
    
    let prometheus_builder = PrometheusBuilder::new()
        .set_buckets(&LATENCY_BUCKET_BOUNDARIES)?;
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    let tcp_builder = TcpBuilder::new()
        .listen_address("127.0.0.1:9999".parse::<std::net::SocketAddr>()?);
    let tcp_recorder = tcp_builder.build()?;
    
    let fanout = FanoutBuilder::default()
        .add_recorder(prometheus_recorder)
        .add_recorder(tcp_recorder)
        .build();
    
    metrics::set_global_recorder(fanout)?;
    
    PROMETHEUS_HANDLE.set(prometheus_handle).map_err(|_| "Metrics already initialized")?;
    
    println!("✅ Volga metrics initialized with Prometheus + TCP fanout");
    println!("📊 Prometheus metrics available via handle");
    println!("🔍 TCP metrics streaming to 127.0.0.1:9999");
    Ok(())
}


pub fn get_stream_task_metrics(vertex_id: VertexId) -> StreamTaskMetrics {
    let handle = PROMETHEUS_HANDLE.get().expect("Metrics not initialized");
    let prometheus_text = handle.render();
    
    parse_stream_task_metrics(&prometheus_text, vertex_id.as_ref())
}

fn parse_stream_task_metrics(prometheus_text: &str, vertex_id: &str) -> StreamTaskMetrics {
    let scrape = Scrape::parse(prometheus_text.lines().map(|line| Ok(line.to_string())))
        .expect("Failed to parse Prometheus metrics");
    
    let mut messages_sent = 0u64;
    let mut messages_recv = 0u64;
    let mut records_sent = 0u64;
    let mut records_recv = 0u64;
    let mut bytes_sent = 0u64;
    let mut bytes_recv = 0u64;
    let mut latency_histogram = vec![0u64; LATENCY_BUCKET_BOUNDARIES.len()];
    let mut backpressure_per_peer = HashMap::new();

    for sample in scrape.samples {
        // Check if this metric belongs to our vertex_id
        if let Some(sample_vertex_id) = sample.labels.get(LABEL_VERTEX_ID) {
            if sample_vertex_id != vertex_id {
                continue;
            }
        } else {
            continue; // Skip metrics without vertex_id label
        }
        
        match sample.value {
            Value::Counter(value) => {
                match sample.metric.as_str() {
                    METRIC_STREAM_TASK_MESSAGES_SENT => messages_sent = value as u64,
                    METRIC_STREAM_TASK_MESSAGES_RECV => messages_recv = value as u64,
                    METRIC_STREAM_TASK_RECORDS_SENT => records_sent = value as u64,
                    METRIC_STREAM_TASK_RECORDS_RECV => records_recv = value as u64,
                    METRIC_STREAM_TASK_BYTES_SENT => bytes_sent = value as u64,
                    METRIC_STREAM_TASK_BYTES_RECV => bytes_recv = value as u64,
                    _ => {}
                }
            }
            Value::Histogram(histogram_counts) => {
                if sample.metric == METRIC_STREAM_TASK_LATENCY {
                    latency_histogram = convert_histogram_counts_to_buckets(&histogram_counts);
                }
            }
            Value::Gauge(value) => {
                if sample.metric == METRIC_STREAM_TASK_BACKPRESSURE_RATIO {
                    if let Some(peer_vertex_id) = sample.labels.get(LABEL_TARGET_VERTEX_ID) {
                        backpressure_per_peer.insert(peer_vertex_id.to_string(), value as f64);
                    }
                }
            }
            _ => {} // Ignore other metric types
        }
    }

    let latency_stats = LatencyMetrics::new(latency_histogram);
    let throughput_stats = ThroughputMetrics::new(messages_sent, messages_recv, records_sent, records_recv, bytes_sent, bytes_recv);
    
    StreamTaskMetrics {
        vertex_id: vertex_id.to_string(),
        latency_stats,
        throughput_stast: throughput_stats,
        backpressure_per_peer
    }
}

/// Convert Prometheus histogram counts to simple bucket format
fn convert_histogram_counts_to_buckets(histogram_counts: &[HistogramCount]) -> Vec<u64> {
    // Filter out the infinity bucket that Prometheus adds automatically
    histogram_counts.iter()
        .filter(|hc| !hc.less_than.is_infinite())
        .map(|hc| hc.count as u64)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use metrics::{counter, histogram};

    /// Helper function to manually build a histogram from latency measurements
    fn build_expected_histogram(latencies: &[f64], boundaries: &[f64]) -> Vec<u64> {
        let mut histogram = vec![0u64; boundaries.len()];
        
        for &latency in latencies {
            for (i, &boundary) in boundaries.iter().enumerate() {
                if latency <= boundary {
                    // Increment all buckets from this one onwards (cumulative)
                    for j in i..boundaries.len() {
                        histogram[j] += 1;
                    }
                    break;
                }
            }
        }
        
        histogram
    }

    #[test]
    fn test_stream_task_metrics_prometheus_parsing() {
        init_metrics();
        
        let vertex_id = "test_task_123".to_string();
        
        // Record stream task metrics
        counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(5);
        counter!(METRIC_STREAM_TASK_MESSAGES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(3);
        counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(25);
        counter!(METRIC_STREAM_TASK_RECORDS_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(15);
        counter!(METRIC_STREAM_TASK_BYTES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(1024);
        counter!(METRIC_STREAM_TASK_BYTES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(512);
        
        // Define latency measurements to record
        let latency_measurements = [5.0, 25.0, 150.0, 2.5, 75.0];
        
        // Record latencies using metrics
        for &latency in &latency_measurements {
            histogram!(METRIC_STREAM_TASK_LATENCY, LABEL_VERTEX_ID => vertex_id.clone()).record(latency);
        }
        
        // Build expected histogram manually
        let expected_histogram = build_expected_histogram(&latency_measurements, &LATENCY_BUCKET_BOUNDARIES);
        
        let parsed_metrics = get_stream_task_metrics(Arc::<str>::from(vertex_id.clone()));
        
        // Verify the parsed stream task metrics
        assert_eq!(parsed_metrics.vertex_id, vertex_id);
        assert_eq!(parsed_metrics.throughput_stast.messages_sent, 5);
        assert_eq!(parsed_metrics.throughput_stast.messages_recv, 3);
        assert_eq!(parsed_metrics.throughput_stast.records_sent, 25);
        assert_eq!(parsed_metrics.throughput_stast.records_recv, 15);
        assert_eq!(parsed_metrics.throughput_stast.bytes_sent, 1024);
        assert_eq!(parsed_metrics.throughput_stast.bytes_recv, 512);
        
        // Verify histogram has data
        assert!(!parsed_metrics.latency_stats.latency_histogram.is_empty(), "Histogram should not be empty");
        
        // Verify exact match between expected and parsed histogram
        assert_eq!(
            parsed_metrics.latency_stats.latency_histogram.len(), 
            expected_histogram.len(),
            "Histogram should have same number of buckets as boundaries"
        );
        
        for (i, (&expected, &actual)) in expected_histogram.iter().zip(parsed_metrics.latency_stats.latency_histogram.iter()).enumerate() {
            assert_eq!(
                actual, expected,
                "Bucket {} mismatch: expected {}, got {} (boundary: {}ms)", 
                i, expected, actual, LATENCY_BUCKET_BOUNDARIES[i]
            );
        }
        
        // Verify we recorded all latency values
        let total_count = parsed_metrics.latency_stats.latency_histogram.last().unwrap_or(&0);
        assert_eq!(*total_count, latency_measurements.len() as u64, 
                   "Should have recorded {} latency measurements", latency_measurements.len());
        
        // Test vertex isolation with a second vertex
        let vertex_b = "task_b".to_string();
        counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_VERTEX_ID => vertex_b.clone()).increment(1);
        counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_VERTEX_ID => vertex_b.clone()).increment(50);
        
        let metrics_b = get_stream_task_metrics(Arc::<str>::from(vertex_b.clone()));
        
        // Verify isolation - vertex B should only see its own metrics
        assert_eq!(metrics_b.vertex_id, vertex_b);
        assert_eq!(metrics_b.throughput_stast.messages_sent, 1);
        assert_eq!(metrics_b.throughput_stast.records_sent, 50);
        assert_eq!(metrics_b.throughput_stast.messages_recv, 0); // Not set for vertex B
        
        // Verify original vertex still has correct metrics
        let metrics_a_again = get_stream_task_metrics(Arc::<str>::from(vertex_id.clone()));
        assert_eq!(metrics_a_again.throughput_stast.messages_sent, 5);
        assert_eq!(metrics_a_again.throughput_stast.records_sent, 25);
    }

}