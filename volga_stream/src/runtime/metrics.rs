use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_exporter_tcp::TcpBuilder;
use metrics_util::layers::FanoutBuilder;
use prometheus_parse::{Scrape, Value, HistogramCount};
use std::{collections::HashMap, sync::{Once, OnceLock}};

use crate::runtime::{execution_graph::ExecutionGraph, operators::operator::OperatorType};

// Global Prometheus handle for programmatic access
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INIT: Once = Once::new();

pub fn init_metrics() {
    INIT.call_once(|| {
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

// Worker metrics
pub const METRIC_WORKER_LATENCY_99: &str = "volga_worker_latency_99";
pub const METRIC_WORKER_LATENCY_95: &str = "volga_worker_latency_95";
pub const METRIC_WORKER_LATENCY_50: &str = "volga_worker_latency_50";
pub const METRIC_WORKER_LATENCY_AVG: &str = "volga_worker_latency_avg";
pub const METRIC_WORKER_SINK_MESSAGES_SENT: &str = "volga_worker_sink_messages_sent";
pub const METRIC_WORKER_SOURCE_MESSAGES_RECV: &str = "volga_worker_source_messages_recv";
pub const METRIC_WORKER_SINK_RECORDS_SENT: &str = "volga_worker_sink_records_sent";
pub const METRIC_WORKER_SOURCE_RECORDS_RECV: &str = "volga_worker_source_records_recv";
pub const METRIC_WORKER_SINK_BYTES_SENT: &str = "volga_worker_sink_bytes_sent";
pub const METRIC_WORKER_SOURCE_BYTES_RECV: &str = "volga_worker_source_bytes_recv";

// Label constants
pub const LABEL_VERTEX_ID: &str = "vertex_id";
pub const LABEL_TARGET_VERTEX_ID: &str = "target_vertex_id";
pub const LABEL_WORKER_ID: &str = "worker_id";
pub const LABEL_OPERATOR_ID: &str = "operator_id";

// Histogram bucket boundaries, milliseconds
pub const LATENCY_BUCKET_BOUNDARIES: [f64; 12] = [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTaskMetrics {
    pub vertex_id: String,
    pub latency_histogram: Vec<u64>,
    pub messages_sent: u64,
    pub messages_recv: u64,
    pub records_sent: u64,
    pub records_recv: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub backpressure_per_peer: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorMetrics {
    pub operator_id: String,
    pub messages_sent: u64,
    pub messages_recv: u64,
    pub records_sent: u64,
    pub records_recv: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
}

impl OperatorMetrics {
    pub fn new(operator_id: String, task_metrics: Vec<StreamTaskMetrics>) -> Self {
        let messages_sent = task_metrics.iter().map(|m| m.messages_sent).sum();
        let messages_recv = task_metrics.iter().map(|m| m.messages_recv).sum();
        let records_sent = task_metrics.iter().map(|m| m.records_sent).sum();
        let records_recv = task_metrics.iter().map(|m| m.records_recv).sum();
        let bytes_sent = task_metrics.iter().map(|m| m.bytes_sent).sum();
        let bytes_recv = task_metrics.iter().map(|m| m.bytes_recv).sum();
        
        OperatorMetrics {
            operator_id,
            messages_sent,
            messages_recv,
            records_sent,
            records_recv,
            bytes_sent,
            bytes_recv
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetrics {
    pub worker_id: String,
    pub latency_histogram: Vec<u64>,
    pub sink_messages_sent: u64,
    pub source_messages_recv: u64,
    pub sink_records_sent: u64,
    pub source_records_recv: u64,
    pub sink_bytes_sent: u64,
    pub source_bytes_recv: u64,
    pub latency_p99: f64,
    pub latency_p95: f64,
    pub latency_p50: f64,
    pub latency_avg: f64,
    pub operator_metrics: HashMap<String, OperatorMetrics>,
    pub tasks_metrics: HashMap<String, StreamTaskMetrics>
}

impl WorkerMetrics {

    pub fn new(worker_id: String, tasks_metrics: HashMap<String, StreamTaskMetrics>, graph: &ExecutionGraph) -> Self {
        let mut operator_metrics = HashMap::new();
        let mut operator_types = HashMap::new();

        let mut metrics_by_operator = HashMap::new();
        for (vertex_id, task_metrics) in tasks_metrics.iter() {
            let operator_id = graph.get_vertex(vertex_id).unwrap().operator_id.clone();
            if !metrics_by_operator.contains_key(&operator_id) {
                metrics_by_operator.insert(operator_id.clone(), Vec::new());
                operator_types.insert(operator_id.clone(), graph.get_vertex_type(vertex_id));
            }
            metrics_by_operator.get_mut(&operator_id).unwrap().push(task_metrics.clone());
        }

        let mut sink_messages_sent = 0;
        let mut source_messages_recv = 0;
        let mut sink_records_sent = 0;
        let mut source_records_recv = 0;
        let mut sink_bytes_sent = 0;
        let mut source_bytes_recv = 0;
        let mut latency_histogram = vec![0u64; LATENCY_BUCKET_BOUNDARIES.len()];

        for (operator_id, task_metrics) in metrics_by_operator.iter() {
            operator_metrics.insert(operator_id.clone(), OperatorMetrics::new(operator_id.clone(), task_metrics.clone()));
            
            let operator_type = operator_types.get(operator_id).unwrap();
            if *operator_type == OperatorType::Sink || *operator_type == OperatorType::ChainedSourceSink {
                sink_messages_sent += task_metrics.iter().map(|m| m.messages_sent).sum::<u64>();
                sink_records_sent += task_metrics.iter().map(|m| m.records_sent).sum::<u64>();
                sink_bytes_sent += task_metrics.iter().map(|m| m.bytes_sent).sum::<u64>();
                for task_metric in task_metrics.iter() {
                    for (i, bucket) in latency_histogram.iter_mut().enumerate() {
                        *bucket += task_metric.latency_histogram[i];
                    }
                }
            }

            if *operator_type == OperatorType::Source || *operator_type == OperatorType::ChainedSourceSink {
                source_messages_recv += task_metrics.iter().map(|m| m.messages_recv).sum::<u64>();
                source_records_recv += task_metrics.iter().map(|m| m.records_recv).sum::<u64>();
                source_bytes_recv += task_metrics.iter().map(|m| m.bytes_recv).sum::<u64>();
            }
        }

        let (latency_p99, latency_p95, latency_p50, latency_avg) = calculate_histogram_stats(&latency_histogram, &LATENCY_BUCKET_BOUNDARIES.to_vec());

        WorkerMetrics {
            worker_id,
            latency_histogram,
            sink_messages_sent,
            source_messages_recv,
            sink_records_sent,
            source_records_recv,
            sink_bytes_sent,
            source_bytes_recv,
            latency_p99,
            latency_p95,
            latency_p50,
            latency_avg,
            operator_metrics,
            tasks_metrics: tasks_metrics
        }
    }

    pub fn record_operator_and_worker_metrics(&self) {
        for (operator_id, operator_metrics) in self.operator_metrics.iter() {
            counter!(METRIC_OPERATOR_MESSAGES_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.messages_sent);
            counter!(METRIC_OPERATOR_MESSAGES_RECV, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.messages_recv);
            counter!(METRIC_OPERATOR_RECORDS_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.records_sent);
            counter!(METRIC_OPERATOR_RECORDS_RECV, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.records_recv);
            counter!(METRIC_OPERATOR_BYTES_SENT, LABEL_OPERATOR_ID => operator_id.clone(), LABEL_WORKER_ID => self.worker_id.clone()).increment(operator_metrics.bytes_sent);
        }

        counter!(METRIC_WORKER_SINK_MESSAGES_SENT, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.sink_messages_sent);
        counter!(METRIC_WORKER_SOURCE_MESSAGES_RECV, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.source_messages_recv);
        counter!(METRIC_WORKER_SINK_RECORDS_SENT, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.sink_records_sent);
        counter!(METRIC_WORKER_SOURCE_RECORDS_RECV, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.source_records_recv);
        counter!(METRIC_WORKER_SINK_BYTES_SENT, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.sink_bytes_sent);
        counter!(METRIC_WORKER_SOURCE_BYTES_RECV, LABEL_WORKER_ID => self.worker_id.clone()).increment(self.source_bytes_recv);

        gauge!(METRIC_WORKER_LATENCY_99, LABEL_WORKER_ID => self.worker_id.clone()).set(self.latency_p99);
        gauge!(METRIC_WORKER_LATENCY_95, LABEL_WORKER_ID => self.worker_id.clone()).set(self.latency_p95);
        gauge!(METRIC_WORKER_LATENCY_50, LABEL_WORKER_ID => self.worker_id.clone()).set(self.latency_p50);
        gauge!(METRIC_WORKER_LATENCY_AVG, LABEL_WORKER_ID => self.worker_id.clone()).set(self.latency_avg);
    }
}

pub fn calculate_histogram_stats(histogram: &Vec<u64>, boundaries: &Vec<f64>) -> (f64, f64, f64, f64) {
    // Handle edge cases
    if histogram.is_empty() || histogram.len() != boundaries.len() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    
    // Get total sample count from last bucket (cumulative histogram)
    let total_samples = *histogram.last().unwrap();
    if total_samples == 0 {
        return (0.0, 0.0, 0.0, 0.0);
    }
    
    // Calculate percentiles
    let p99 = calculate_percentile(histogram, boundaries, total_samples, 99.0);
    let p95 = calculate_percentile(histogram, boundaries, total_samples, 95.0);
    let p50 = calculate_percentile(histogram, boundaries, total_samples, 50.0);
    
    // Calculate average using weighted sum of bucket midpoints
    let avg = calculate_weighted_average(histogram, boundaries, total_samples);
    
    (p99, p95, p50, avg)
}

fn calculate_percentile(histogram: &Vec<u64>, boundaries: &Vec<f64>, total_samples: u64, percentile: f64) -> f64 {
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

fn calculate_weighted_average(histogram: &Vec<u64>, boundaries: &Vec<f64>, total_samples: u64) -> f64 {
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
    
    register_metric_descriptions();
    
    println!("✅ Volga metrics initialized with Prometheus + TCP fanout");
    println!("📊 Prometheus metrics available via handle");
    println!("🔍 TCP metrics streaming to 127.0.0.1:9999");
    Ok(())
}

fn register_metric_descriptions() {
    metrics::describe_counter!(METRIC_STREAM_TASK_MESSAGES_SENT, "Number of messages sent by stream task");
    metrics::describe_counter!(METRIC_STREAM_TASK_MESSAGES_RECV, "Number of messages received by stream task");
    metrics::describe_counter!(METRIC_STREAM_TASK_RECORDS_SENT, "Number of records sent by stream task");
    metrics::describe_counter!(METRIC_STREAM_TASK_RECORDS_RECV, "Number of records received by stream task");
    metrics::describe_counter!(METRIC_STREAM_TASK_BYTES_SENT, "Number of bytes sent by stream task");
    metrics::describe_counter!(METRIC_STREAM_TASK_BYTES_RECV, "Number of bytes received by stream task");
    metrics::describe_histogram!(METRIC_STREAM_TASK_LATENCY, metrics::Unit::Milliseconds, "Processing latency of stream task");
}

pub fn get_stream_task_metrics(vertex_id: String) -> StreamTaskMetrics {
    let handle = PROMETHEUS_HANDLE.get().expect("Metrics not initialized");
    let prometheus_text = handle.render();
    
    parse_stream_task_metrics(&prometheus_text, &vertex_id)
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
    let mut latency_buckets = vec![0u64; LATENCY_BUCKET_BOUNDARIES.len()];
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
                    latency_buckets = convert_histogram_counts_to_buckets(&histogram_counts);
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
    
    StreamTaskMetrics {
        vertex_id: vertex_id.to_string(),
        latency_histogram: latency_buckets,
        messages_sent,
        messages_recv,
        records_sent,
        records_recv,
        bytes_sent,
        bytes_recv,
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
        
        let parsed_metrics = get_stream_task_metrics(vertex_id.clone());
        
        // Verify the parsed stream task metrics
        assert_eq!(parsed_metrics.vertex_id, vertex_id);
        assert_eq!(parsed_metrics.messages_sent, 5);
        assert_eq!(parsed_metrics.messages_recv, 3);
        assert_eq!(parsed_metrics.records_sent, 25);
        assert_eq!(parsed_metrics.records_recv, 15);
        assert_eq!(parsed_metrics.bytes_sent, 1024);
        assert_eq!(parsed_metrics.bytes_recv, 512);
        
        // Verify histogram has data
        assert!(!parsed_metrics.latency_histogram.is_empty(), "Histogram should not be empty");
        
        // Verify exact match between expected and parsed histogram
        assert_eq!(
            parsed_metrics.latency_histogram.len(), 
            expected_histogram.len(),
            "Histogram should have same number of buckets as boundaries"
        );
        
        for (i, (&expected, &actual)) in expected_histogram.iter().zip(parsed_metrics.latency_histogram.iter()).enumerate() {
            assert_eq!(
                actual, expected,
                "Bucket {} mismatch: expected {}, got {} (boundary: {}ms)", 
                i, expected, actual, LATENCY_BUCKET_BOUNDARIES[i]
            );
        }
        
        // Verify we recorded all latency values
        let total_count = parsed_metrics.latency_histogram.last().unwrap_or(&0);
        assert_eq!(*total_count, latency_measurements.len() as u64, 
                   "Should have recorded {} latency measurements", latency_measurements.len());
        
        // Test vertex isolation with a second vertex
        let vertex_b = "task_b".to_string();
        counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_VERTEX_ID => vertex_b.clone()).increment(1);
        counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_VERTEX_ID => vertex_b.clone()).increment(50);
        
        let metrics_b = get_stream_task_metrics(vertex_b.clone());
        
        // Verify isolation - vertex B should only see its own metrics
        assert_eq!(metrics_b.vertex_id, vertex_b);
        assert_eq!(metrics_b.messages_sent, 1);
        assert_eq!(metrics_b.records_sent, 50);
        assert_eq!(metrics_b.messages_recv, 0); // Not set for vertex B
        
        // Verify original vertex still has correct metrics
        let metrics_a_again = get_stream_task_metrics(vertex_id.clone());
        assert_eq!(metrics_a_again.messages_sent, 5);
        assert_eq!(metrics_a_again.records_sent, 25);
    }

}