use crate::{
    api::pipeline_context::{PipelineContext, PipelineContextBuilder},
    common::test_utils::{gen_unique_grpc_port, print_pipeline_state},
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::source::word_count_source::BatchingMode, master::PipelineState, metrics::{LATENCY_BUCKET_BOUNDARIES, LatencyMetrics}, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, WordCountSourceConfig}}, worker::WorkerState
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use core::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap};
use arrow::{array::StringArray, datatypes::{Field, Schema}};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

// TODO we need to rewrite the way we measure latency and throughput - 
// aggregate operator buffers messages adding latency
// and also emits only once (or at regular intervals)

#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub messages_per_second: Vec<f64>,
    pub records_per_second: Vec<f64>,
    pub latency_histogram: Option<Vec<u64>>, // in milliseconds
    pub timestamps: Vec<u64>, // Unix timestamp in seconds
}

impl BenchmarkMetrics {
    pub fn new() -> Self {
        Self {
            messages_per_second: Vec::new(),
            records_per_second: Vec::new(),
            latency_histogram: None,
            timestamps: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, messages_per_sec: f64, records_per_sec: f64, latency_ms: f64) {
        self.messages_per_second.push(messages_per_sec);
        self.records_per_second.push(records_per_sec);
        // self.latency_samples.push(latency_ms);
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

    pub fn calculate_latency_stats(&self) -> Option<LatencyMetrics> {
        if self.latency_histogram.is_none() {
            return None;
        }

        Some(LatencyMetrics::new(self.latency_histogram.clone().unwrap()))
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

async fn poll_pipeline_state_updates(
    state_updates_receiver: &mut mpsc::Receiver<PipelineState>,
    benchmark_metrics: &mut BenchmarkMetrics,
    last_metrics: &mut Option<(u64, u64)>, // (messages, records)
    last_timestamp: &mut Instant,
    source_operator_id: String,
    sink_operator_id: String,
) -> Option<PipelineState> {
    let current_time = Instant::now();
    let pipeline_state = match state_updates_receiver.recv().await {
        Some(state) => state,
        None => return None, // Channel closed, execution finished
    };
    
    let worker_metrics = pipeline_state.worker_states.values().next().unwrap().worker_metrics.as_ref().unwrap();
    let source_operator_metrics = worker_metrics.operator_metrics.get(&source_operator_id).unwrap();
    let sink_operator_metrics = worker_metrics.operator_metrics.get(&sink_operator_id).unwrap();
    let current_messages = source_operator_metrics.throughput_metrics.messages_recv;
    let current_records = source_operator_metrics.throughput_metrics.records_recv;
    
    if let Some((last_messages, last_records)) = *last_metrics {
        let time_diff = current_time.duration_since(*last_timestamp).as_secs_f64();
        if time_diff > 0.0 {
            let messages_diff = current_messages - last_messages;
            let records_diff = current_records - last_records;
            
            let messages_per_sec = messages_diff as f64 / time_diff;
            let records_per_sec = records_diff as f64 / time_diff;
            
            // Calculate average latency from histogram
            let latency_ms = sink_operator_metrics.latency_metrics.avg;

            benchmark_metrics.add_sample(messages_per_sec, records_per_sec, latency_ms);
        }
    }
    
    *last_metrics = Some((current_messages, current_records));
    *last_timestamp = current_time;
    Some(pipeline_state.clone())
}

pub async fn run_word_count_benchmark(
    parallelism: usize,
    word_length: usize,
    dictionary_size_per_source: usize,
    run_for_s: u64,
    batch_size: usize,
    polling_interval_ms: u64,
    batching_mode: BatchingMode,
) -> Result<(HashMap<String, f64>, BenchmarkMetrics)> {
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Create streaming context using SQL instead of manual operator configuration
    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_source(
            "word_count_source".to_string(), 
            SourceConfig::WordCountSourceConfig(WordCountSourceConfig::new(
                word_length,
                dictionary_size_per_source,
                None, // Use time-based instead
                Some(run_for_s),
                batch_size,
                batching_mode,
            )), 
            Arc::new(Schema::new(vec![
                Field::new("word", arrow::datatypes::DataType::Utf8, false),
                Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            ]))
        )
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .sql("SELECT word, COUNT(*) as count FROM word_count_source GROUP BY word")
        .with_executor(Box::new(LocalExecutor::new()))
        .build();

    let logical_graph = context.get_logical_graph().unwrap();

    let source_operator_id = logical_graph.get_nodes_by_predicate(|node| matches!(node.operator_config, OperatorConfig::SourceConfig(_))).first().unwrap().operator_id.clone();
    let sink_operator_id = logical_graph.get_nodes_by_predicate(|node| matches!(node.operator_config, OperatorConfig::SinkConfig(_))).first().unwrap().operator_id.clone();

    let (state_updates_sender, mut state_updates_receiver) = mpsc::channel(100);
    let running = Arc::new(AtomicBool::new(true));

    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    // Start metrics polling task
    let benchmark_metrics = Arc::new(Mutex::new(BenchmarkMetrics::new()));
    let benchmark_metrics_clone = benchmark_metrics.clone();
    
    let running_clone = running.clone();
    let state_updates_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(polling_interval_ms));
        let mut last_metrics: Option<(u64, u64)> = None;
        let mut last_timestamp = Instant::now();
        let mut last_print_timestamp = Instant::now();
        
        while running_clone.load(Ordering::SeqCst) {
            interval.tick().await;
            
            {
                let mut metrics_guard = benchmark_metrics_clone.lock().await;
                
                let pipeline_state = match poll_pipeline_state_updates(
                    &mut state_updates_receiver,
                    &mut *metrics_guard,
                    &mut last_metrics,
                    &mut last_timestamp,
                    source_operator_id.clone(),
                    sink_operator_id.clone(),
                ).await {
                    Some(state) => state,
                    None => break, // Channel closed, execution finished
                };

                let now = Instant::now();
                if now.duration_since(last_print_timestamp).as_secs() >= 1 {
                    println!("[{}] Worker State", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
                    print_pipeline_state(&pipeline_state, None, false, false);
                    last_print_timestamp = now;
                }
            }
        }
    });
    
    // Execute using StreamingContext with metrics
    context.execute_with_state_updates(Some(state_updates_sender)).await.unwrap();
    running.store(false, Ordering::Relaxed);

    // Wait for metrics task to complete
    let _ = state_updates_task.await;
    
    // Get final metrics
    let benchmark_metrics = benchmark_metrics.lock().await.clone();
    
    // Get results using the same approach as word_count_test
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let result_vec = client.get_vector().await.unwrap();
    storage_server.stop().await;

    // Process results the same way as word_count_test
    let mut word_counts = HashMap::new();
    
    for (batch_idx, message) in result_vec.iter().enumerate() {
        let batch = message.record_batch();
        
        // Verify we have the expected number of columns
        if batch.num_columns() != 2 {
            println!("Warning: Batch {} has {} columns instead of 2", batch_idx, batch.num_columns());
            continue;
        }
        
        // Extract columns from the batch
        let word_column = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let count_column = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        
        // Process each row in this batch
        for i in 0..batch.num_rows() {
            let word = word_column.value(i).to_string();
            let count = count_column.value(i);
            
            // Accumulate counts (in case the same word appears in multiple batches)
            *word_counts.entry(word).or_insert(0) += count;
        }
        
        println!("Batch {}: {} rows", batch_idx, batch.num_rows());
    }
    
    // Convert i64 counts to f64 for compatibility with existing benchmark code
    let word_counts: HashMap<String, f64> = word_counts.into_iter().map(|(k, v)| (k, v as f64)).collect();

    Ok((word_counts, benchmark_metrics))
}

#[tokio::test]
async fn test_word_count_benchmark() -> Result<()> {
    let parallelism = 1;
    let word_length = 10;
    let dictionary_size_per_source = 1000;
    let run_for_s = 10;
    let batch_size = 1000;
    let polling_interval_ms = 100; // Poll every 100ms
    let batching_mode = BatchingMode::RoundRobin;

    let (word_counts, benchmark_metrics) = run_word_count_benchmark(
        parallelism,
        word_length,
        dictionary_size_per_source,
        run_for_s,
        batch_size,
        polling_interval_ms,
        batching_mode
    ).await?;

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
    if let Some(latency_metrics) = benchmark_metrics.calculate_latency_stats() {
        println!("\nLatency Statistics (ms):");
        println!("  P99: {}", latency_metrics.p99);
        println!("  P95: {}", latency_metrics.p95);
        println!("  P50: {}", latency_metrics.p50);
        println!("  Avg: {}", latency_metrics.avg);
    }
    
    println!("\nSample Count: {}", benchmark_metrics.messages_per_second.len());
    println!("=====================================\n");

    // Basic assertions
    assert_eq!(word_counts.len(), dictionary_size_per_source * parallelism, 
        "Should have exactly num_words * parallelism unique words");
    
    // Verify we have some metrics
    assert!(!benchmark_metrics.messages_per_second.is_empty(), 
        "Should have collected some throughput metrics");
    assert!(!benchmark_metrics.latency_histogram.is_some(), 
        "Should have collected some latency metrics");

    Ok(())
} 