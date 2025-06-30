use crate::{common::test_utils::{gen_unique_grpc_port, print_worker_metrics}, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        reduce::{AggregationResultExtractor, AggregationType, ReduceFunction},
        source::word_count_source::{BatchingMode, WordCountSourceFunction},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::SourceConfig}, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, KeyedMessage};
use crate::common::Key;
use crate::runtime::tests::graph_test_utils::{create_test_execution_graph, TestGraphConfig};
use crate::runtime::worker::WorkerState;
use anyhow::Result;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use kameo::{Actor, spawn};
use crate::transport::channel::Channel;
use async_trait::async_trait;
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub messages_per_second: Vec<f64>,
    pub records_per_second: Vec<f64>,
    pub latency_samples: Vec<u64>, // in milliseconds
    pub timestamps: Vec<u64>, // Unix timestamp in seconds
}

impl BenchmarkMetrics {
    pub fn new() -> Self {
        Self {
            messages_per_second: Vec::new(),
            records_per_second: Vec::new(),
            latency_samples: Vec::new(),
            timestamps: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, messages_per_sec: f64, records_per_sec: f64, latency_ms: u64) {
        self.messages_per_second.push(messages_per_sec);
        self.records_per_second.push(records_per_sec);
        self.latency_samples.push(latency_ms);
        self.timestamps.push(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
    }

    pub fn calculate_throughput_stats(&self) -> ThroughputStats {
        if self.messages_per_second.is_empty() {
            return ThroughputStats::default();
        }

        let messages = &self.messages_per_second;
        let records = &self.records_per_second;

        ThroughputStats {
            messages_max: messages.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)),
            messages_min: messages.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
            messages_mean: messages.iter().sum::<f64>() / messages.len() as f64,
            messages_stddev: calculate_stddev(messages),
            records_max: records.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)),
            records_min: records.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
            records_mean: records.iter().sum::<f64>() / records.len() as f64,
            records_stddev: calculate_stddev(records),
        }
    }

    pub fn calculate_latency_stats(&self) -> LatencyStats {
        if self.latency_samples.is_empty() {
            return LatencyStats::default();
        }

        let mut sorted_latencies = self.latency_samples.clone();
        sorted_latencies.sort_unstable();

        let len = sorted_latencies.len();
        let p50_idx = (len as f64 * 0.5) as usize;
        let p95_idx = (len as f64 * 0.95) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        LatencyStats {
            p50: sorted_latencies[p50_idx.min(len - 1)],
            p95: sorted_latencies[p95_idx.min(len - 1)],
            p99: sorted_latencies[p99_idx.min(len - 1)],
            avg: sorted_latencies.iter().sum::<u64>() / len as u64,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ThroughputStats {
    pub messages_max: f64,
    pub messages_min: f64,
    pub messages_mean: f64,
    pub messages_stddev: f64,
    pub records_max: f64,
    pub records_min: f64,
    pub records_mean: f64,
    pub records_stddev: f64,
}

#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub avg: u64,
}

fn calculate_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>() / (values.len() - 1) as f64;
    
    variance.sqrt()
}

async fn poll_worker_metrics(
    worker: &Worker,
    benchmark_metrics: &mut BenchmarkMetrics,
    last_metrics: &mut Option<(u64, u64)>, // (messages, records)
    last_timestamp: &mut Instant,
) {
    let current_time = Instant::now();
    let worker_state = worker.get_state().await;
    
    let current_messages = worker_state.aggregated_metrics.total_messages;
    let current_records = worker_state.aggregated_metrics.total_records;
    
    if let Some((last_messages, last_records)) = *last_metrics {
        let time_diff = current_time.duration_since(*last_timestamp).as_secs_f64();
        if time_diff > 0.0 {
            let messages_diff = current_messages - last_messages;
            let records_diff = current_records - last_records;
            
            let messages_per_sec = messages_diff as f64 / time_diff;
            let records_per_sec = records_diff as f64 / time_diff;
            
            // Calculate average latency from histogram
            let latency_ms = calculate_average_latency(&worker_state.aggregated_metrics.latency_histogram);
            
            benchmark_metrics.add_sample(messages_per_sec, records_per_sec, latency_ms);
        }
    }

    // TODO print worker state
    print_worker_metrics(&worker_state);
    
    *last_metrics = Some((current_messages, current_records));
    *last_timestamp = current_time;
}

fn calculate_average_latency(histogram: &[u64]) -> u64 {
    if histogram.len() != 5 {
        return 0;
    }
    
    let total_samples: u64 = histogram.iter().sum();
    if total_samples == 0 {
        return 0;
    }
    
    // Histogram buckets: [0-1ms, 1-10ms, 10-100ms, 100ms-1s, >1s]
    let bucket_centers = [0, 5, 55, 550, 1000]; // Approximate centers
    let weighted_sum: u64 = histogram.iter()
        .zip(bucket_centers.iter())
        .map(|(&count, &center)| count * center)
        .sum();
    
    weighted_sum / total_samples
}

pub async fn run_word_count_benchmark(
    parallelism: usize,
    word_length: usize,
    dictionary_size_per_source: usize,
    run_for_s: u64,
    batch_size: usize,
    polling_interval_ms: u64,
) -> Result<(HashMap<String, f64>, BenchmarkMetrics)> {
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Define operator chain: source -> keyby -> reduce -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::WordCountSourceConfig {
            word_length: word_length,
            dictionary_size: dictionary_size_per_source,
            num_to_send_per_word: None, // Use time-based instead
            run_for_s: Some(run_for_s),
            batch_size: batch_size,
            batching_mode: BatchingMode::SameWord,
        })),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["word".to_string()]))),
        ("reduce".to_string(), OperatorConfig::ReduceConfig(
            ReduceFunction::new_arrow_reduce("word".to_string()),
            Some(AggregationResultExtractor::single_aggregation(
                AggregationType::Count,
                "count".to_string(),
            )),
        )),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))),
    ];

    let (graph, _) = create_test_execution_graph(TestGraphConfig {
        operators,
        parallelism,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    });

    let vertex_ids = graph.get_vertices().keys().cloned().collect();
    
    // Create and start worker
    let worker_config = WorkerConfig::new(
        graph,
        vertex_ids,
        1,
        TransportBackendType::InMemory,
    );
    let worker = Worker::new(worker_config);
    let worker = Arc::new(Mutex::new(worker));

    let (result_map, benchmark_metrics) = tokio::spawn(async move {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        
        // Start metrics polling task
        let benchmark_metrics = Arc::new(Mutex::new(BenchmarkMetrics::new()));
        let benchmark_metrics_clone = benchmark_metrics.clone();
        let worker_clone = worker.clone();
        
        let metrics_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(polling_interval_ms));
            let mut last_metrics: Option<(u64, u64)> = None;
            let mut last_timestamp = Instant::now();
            
            loop {
                interval.tick().await;
                
                {
                    let worker_guard = worker_clone.lock().await;
                    let mut metrics_guard = benchmark_metrics_clone.lock().await;
                    
                    poll_worker_metrics(
                        &worker_guard,
                        &mut *metrics_guard,
                        &mut last_metrics,
                        &mut last_timestamp,
                    ).await;
                }
                
                // Check if all tasks are finished
                {
                    let worker_guard = worker_clone.lock().await;
                    let state = worker_guard.get_state().await;
                    if state.all_tasks_have_status(crate::runtime::stream_task::StreamTaskStatus::Finished) {
                        break;
                    }
                }
            }
        });
        
        // Execute the worker lifecycle (similar to word count test)
        {
            let mut worker_guard = worker.lock().await;
            worker_guard.execute_worker_lifecycle_for_testing().await;
        }
        
        // Wait for metrics task to complete
        let _ = metrics_task.await;
        
        // Get final metrics
        let benchmark_metrics = benchmark_metrics.lock().await.clone();
        
        // Get results
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_map = client.get_map().await.unwrap();
        storage_server.stop().await;
        
        (result_map, benchmark_metrics)
    }).await.unwrap();

    // Process results
    let mut word_counts = HashMap::new();
    for (_, batch) in result_map {
        let keyed_batch = match batch {
            Message::Keyed(kb) => kb,
            _ => panic!("Expected KeyedBatch"),
        };
        
        let key_batch = keyed_batch.key().record_batch();
        let word_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let word = word_array.value(0).to_string();
        
        let count_array = keyed_batch.base.record_batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let count = count_array.value(0);
        
        word_counts.insert(word, count);
    }

    Ok((word_counts, benchmark_metrics))
}

#[test]
fn test_word_count_benchmark() -> Result<()> {
    let runtime = Runtime::new()?;
    
    let parallelism = 4;
    let word_length = 10;
    let dictionary_size_per_source = 2;
    let run_for_s = 20; // Run for 10 seconds
    let batch_size = 10;
    let polling_interval_ms = 1000; // Poll every 100ms

    let (word_counts, benchmark_metrics) = runtime.block_on(async {
        run_word_count_benchmark(
            parallelism,
            word_length,
            dictionary_size_per_source,
            run_for_s,
            batch_size,
            polling_interval_ms,
        ).await
    })?;

    // Print benchmark results
    println!("\n=== Word Count Benchmark Results ===");
    println!("Configuration:");
    println!("  Parallelism: {}", parallelism);
    println!("  Word Length: {}", word_length);
    println!("  Dictionary Size per Source: {}", dictionary_size_per_source);
    println!("  Run Duration: {} seconds", run_for_s);
    println!("  Batch Size: {}", batch_size);
    println!("  Polling Interval: {}ms", polling_interval_ms);
    
    println!("\nResults:");
    println!("  Total Unique Words: {}", word_counts.len());
    println!("  Expected Words: {}", dictionary_size_per_source * parallelism);
    
    // Calculate and print throughput statistics
    let throughput_stats = benchmark_metrics.calculate_throughput_stats();
    println!("\nThroughput Statistics:");
    println!("  Messages/sec:");
    println!("    Max: {:.2}", throughput_stats.messages_max);
    println!("    Min: {:.2}", throughput_stats.messages_min);
    println!("    Mean: {:.2}", throughput_stats.messages_mean);
    println!("    StdDev: {:.2}", throughput_stats.messages_stddev);
    println!("  Records/sec:");
    println!("    Max: {:.2}", throughput_stats.records_max);
    println!("    Min: {:.2}", throughput_stats.records_min);
    println!("    Mean: {:.2}", throughput_stats.records_mean);
    println!("    StdDev: {:.2}", throughput_stats.records_stddev);
    
    // Calculate and print latency statistics
    let latency_stats = benchmark_metrics.calculate_latency_stats();
    println!("\nLatency Statistics (ms):");
    println!("  P50: {}", latency_stats.p50);
    println!("  P95: {}", latency_stats.p95);
    println!("  P99: {}", latency_stats.p99);
    println!("  Avg: {}", latency_stats.avg);
    
    println!("\nSample Count: {}", benchmark_metrics.messages_per_second.len());
    println!("=====================================\n");

    // Basic assertions
    assert_eq!(word_counts.len(), dictionary_size_per_source * parallelism, 
        "Should have exactly num_words * parallelism unique words");
    
    // Verify we have some metrics
    assert!(!benchmark_metrics.messages_per_second.is_empty(), 
        "Should have collected some throughput metrics");
    assert!(!benchmark_metrics.latency_samples.is_empty(), 
        "Should have collected some latency metrics");

    Ok(())
} 