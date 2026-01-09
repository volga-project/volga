use crate::{
    api::pipeline_context::{ExecutionMode, ExecutionProfile, PipelineContextBuilder},
    common::test_utils::{gen_unique_grpc_port, print_pipeline_state},
    runtime::metrics::PipelineStateHistory,
    runtime::{
        functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator},
        master::PipelineState,
        operators::{
            sink::sink_operator::SinkConfig,
            source::source_operator::SourceConfig,
        },
    },
    storage::InMemoryStorageServer,
};
use anyhow::Result;
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use datafusion::common::ScalarValue;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

fn create_datagen_config(
    rate: Option<f32>,
    run_for_s: Option<f64>,
    batch_size: usize,
    start_ms: i64,
    step_ms: i64,
    num_unique_keys: usize,
    value_start: f64,
    value_step: f64,
) -> DatagenSourceConfig {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let fields = HashMap::from([
        ("event_time".to_string(), FieldGenerator::IncrementalTimestamp {
            start_ms: start_ms,
            step_ms: step_ms,
        }),
        ("key".to_string(), FieldGenerator::Key {
            num_unique: num_unique_keys,
        }),
        ("value".to_string(), FieldGenerator::Increment {
            start: ScalarValue::Float64(Some(value_start)),
            step: ScalarValue::Float64(Some(value_step)),
        }),
    ]);

    DatagenSourceConfig::new(schema, rate, None, run_for_s, batch_size, fields)
}


#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub execution_time: Duration,
    pub requests_processed: usize,
    pub requests_per_second: f64,
    pub history: PipelineStateHistory,
}

impl BenchmarkMetrics {
    pub fn new() -> Self {
        Self {
            execution_time: Duration::from_secs(0),
            requests_processed: 0,
            requests_per_second: 0.0,
            history: PipelineStateHistory::new(),
        }
    }

    pub fn add_sample(&mut self, timestamp: u64, pipeline_state: PipelineState) {
        self.history.add_sample(timestamp, pipeline_state);
    }
}

async fn poll_pipeline_state_updates(
    state_updates_receiver: &mut mpsc::Receiver<PipelineState>,
    benchmark_metrics: &mut BenchmarkMetrics,
) -> Option<PipelineState> {
    let pipeline_state = match state_updates_receiver.recv().await {
        Some(state) => state,
        None => return None,
    };
    
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    benchmark_metrics.add_sample(timestamp, pipeline_state.clone());
    
    Some(pipeline_state)
}

pub async fn run_window_request_benchmark(
    parallelism: usize,
    num_unique_keys: usize,
    run_for_s: f64,
    datagen_rate: Option<f32>,
    request_rate: Option<f32>,
    batch_size: usize,
    polling_interval_ms: u64,
    throughput_window_seconds: Option<u64>,
) -> Result<BenchmarkMetrics> {
    let start_ms = 1000;
    let step_ms = 1000;
    let value_start = 1.0;
    let value_step = 0.001;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Datagen source for query input (builds window state)
    let query_datagen_config = create_datagen_config(
        datagen_rate,
        Some(run_for_s),
        batch_size,
        start_ms,
        step_ms,
        num_unique_keys,
        value_start,
        value_step,
    );
    let schema = query_datagen_config.schema.clone();

    // Datagen source for request input (generates requests)
    let request_datagen_config = create_datagen_config(
        request_rate,
        Some(run_for_s),
        batch_size,
        start_ms,
        step_ms,
        num_unique_keys,
        value_start,
        value_step,
    );

    let sql = "SELECT 
        event_time,
        key,
        value,
        SUM(value) OVER w as sum_value,
        COUNT(value) OVER w as count_value,
        AVG(value) OVER w as avg_value,
        MIN(value) OVER w as min_value,
        MAX(value) OVER w as max_value
    FROM events
    WINDOW w AS (
        PARTITION BY key 
        ORDER BY event_time 
        RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
    )";

    let benchmark_start = Instant::now();

    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_source(
            "events".to_string(),
            SourceConfig::DatagenSourceConfig(query_datagen_config),
            schema.clone()
        )
        .with_request_source_sink(
            SourceConfig::DatagenSourceConfig(request_datagen_config),
            Some(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        )
        .sql(sql)
        .with_execution_profile(ExecutionProfile::SingleWorkerNoMaster { num_threads_per_task: 4 })
        .with_execution_mode(ExecutionMode::Request)
        .build();

    let (state_updates_sender, mut state_updates_receiver) = mpsc::channel(100);
    let running = Arc::new(AtomicBool::new(true));

    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    let benchmark_metrics = Arc::new(Mutex::new(BenchmarkMetrics::new()));
    let benchmark_metrics_clone = benchmark_metrics.clone();
    
    let running_clone = running.clone();
    let throughput_window_seconds_clone = throughput_window_seconds;
    let state_updates_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(polling_interval_ms));
        let mut last_print_timestamp = Instant::now();
        let mut history = PipelineStateHistory::new();
        
        while running_clone.load(Ordering::SeqCst) {
            interval.tick().await;
            
            {
                let mut metrics_guard = benchmark_metrics_clone.lock().await;
                
                let pipeline_state = match poll_pipeline_state_updates(
                    &mut state_updates_receiver,
                    &mut *metrics_guard,
                ).await {
                    Some(state) => state,
                    None => break,
                };

                // Add to history
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                history.add_sample(timestamp, pipeline_state.clone());

                let now = Instant::now();
                if now.duration_since(last_print_timestamp).as_secs() >= 1 {
                    println!("[{}] Worker State", timestamp);
                    
                    print_pipeline_state(
                        &pipeline_state, 
                        Some(&["Window_1".to_string()]), 
                        true, 
                        false,
                        Some(&history),
                        throughput_window_seconds_clone,
                    );
                    last_print_timestamp = now;
                }
            }
        }
    });
    
    context.execute_with_state_updates(Some(state_updates_sender)).await?;
    running.store(false, Ordering::Relaxed);

    let _ = state_updates_task.await;
    
    let mut benchmark_metrics = (*benchmark_metrics.lock().await).clone();
    benchmark_metrics.execution_time = benchmark_start.elapsed();
    
    storage_server.stop().await;

    // let requests_processed = if let Some(rate) = request_rate {
    //     (rate * run_for_s as f32) as usize
    // } else {
    //     benchmark_metrics.samples.len() * batch_size
    // };

    // benchmark_metrics.requests_processed = requests_processed;
    // benchmark_metrics.requests_per_second = if benchmark_metrics.execution_time.as_secs_f64() > 0.0 {
    //     requests_processed as f64 / benchmark_metrics.execution_time.as_secs_f64()
    // } else {
    //     0.0
    // };

    Ok(benchmark_metrics)
}

// #[tokio::test]
async fn test_window_request_benchmark() -> Result<()> {
    let parallelism = 4;
    let num_unique_keys = 10;
    let run_for_s = 10.0;
    let datagen_rate = None;
    let request_rate = Some(100.0);
    let batch_size = 1000;
    let polling_interval_ms = 100;
    let throughput_window_seconds = Some(5);

    let metrics = run_window_request_benchmark(
        parallelism,
        num_unique_keys,
        run_for_s,
        datagen_rate,
        request_rate,
        batch_size,
        polling_interval_ms,
        throughput_window_seconds,
    ).await?;

    println!("\n=== Window Request Operator Benchmark Results ===");
    println!("Configuration:");
    println!("  Parallelism: {}", parallelism);
    println!("  Number of Keys: {}", num_unique_keys);
    println!("  Run Duration: {}s", run_for_s);
    println!("  Query Datagen Rate: {:?}", datagen_rate);
    println!("  Request Datagen Rate: {:?}", request_rate);
    println!("  Batch Size: {}", batch_size);
    println!("  Polling Interval: {}ms", polling_interval_ms);

    println!("\nResults:");
    println!("  Execution Time: {:?}", metrics.execution_time);
    println!("  Requests Processed: {}", metrics.requests_processed);
    println!("  Requests Per Second: {:.2}", metrics.requests_per_second);
    println!("  Sample Count: {}", metrics.history.samples.len());
    
    // Print final aggregated statistics
    let final_stats = metrics.history.final_stats();
    
    if !final_stats.throughput_per_task.is_empty() {
        println!("\nThroughput Statistics (per task, aggregated over all history):");
        for (task_id, task_stats) in &final_stats.throughput_per_task {
            println!("  Task: {}", task_id);
            println!("    Messages Sent: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} msg/s", 
                task_stats.messages_sent.avg, task_stats.messages_sent.min, 
                task_stats.messages_sent.max, task_stats.messages_sent.stddev);
            println!("    Messages Recv: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} msg/s", 
                task_stats.messages_recv.avg, task_stats.messages_recv.min, 
                task_stats.messages_recv.max, task_stats.messages_recv.stddev);
            println!("    Records Sent: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} rec/s", 
                task_stats.records_sent.avg, task_stats.records_sent.min, 
                task_stats.records_sent.max, task_stats.records_sent.stddev);
            println!("    Records Recv: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} rec/s", 
                task_stats.records_recv.avg, task_stats.records_recv.min, 
                task_stats.records_recv.max, task_stats.records_recv.stddev);
            println!("    Bytes Sent: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} B/s", 
                task_stats.bytes_sent.avg, task_stats.bytes_sent.min, 
                task_stats.bytes_sent.max, task_stats.bytes_sent.stddev);
            println!("    Bytes Recv: avg={:.2}, min={:.2}, max={:.2}, stddev={:.2} B/s", 
                task_stats.bytes_recv.avg, task_stats.bytes_recv.min, 
                task_stats.bytes_recv.max, task_stats.bytes_recv.stddev);
        }
    }
    
    if !final_stats.latency_per_task.is_empty() {
        println!("\nLatency Statistics (per task, merged histogram over all history):");
        for (task_id, latency_stats) in &final_stats.latency_per_task {
            println!("  Task: {}", task_id);
            println!("    Avg: {:.2} ms", latency_stats.avg);
            println!("    P50: {:.2} ms", latency_stats.p50);
            println!("    P95: {:.2} ms", latency_stats.p95);
            println!("    P99: {:.2} ms", latency_stats.p99);
        }
    }
    
    println!("==========================================\n");

    assert!(metrics.requests_processed > 0, "Should have processed at least some requests");
    assert!(metrics.execution_time.as_secs_f64() > 0.0, "Execution time should be positive");

    Ok(())
}

