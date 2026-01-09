#![cfg(test)]
#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

use crate::{
    api::pipeline_context::{ExecutionMode, PipelineContextBuilder},
    common::test_utils::{gen_unique_grpc_port, print_pipeline_state},
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator},
        master::PipelineState,
        metrics::PipelineStateHistory,
        operators::{
            sink::sink_operator::SinkConfig,
            source::source_operator::SourceConfig,
            window::{TileConfig, TimeGranularity, window_operator::ExecutionMode as WindowExecutionMode},
        },
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use std::collections::HashMap;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use core::sync::atomic::{AtomicBool, Ordering};
use datafusion::common::ScalarValue;
use tokio::sync::{Mutex, mpsc};
use petgraph::prelude::NodeIndex;

#[derive(Debug, Clone)]
pub enum WindowType {
    Range { milliseconds: i64 },
    Rows { preceding: i64 },
}

#[derive(Debug, Clone)]
pub enum AggregationType {
    Plain,      // MIN, MAX
    Retractable, // SUM, COUNT, AVG
    Mixed,      // Combination of both
}

#[derive(Debug, Clone)]
pub struct WindowBenchmarkConfig {
    // Basic params
    pub parallelism: usize,
    pub num_keys: usize,
    pub total_records: Option<usize>,
    pub batch_size: usize,
    pub rate: Option<f32>,
    pub run_for_s: Option<f64>,
    
    // Window params
    pub window_type: WindowType,
    pub num_windows: usize, // Number of window functions in query (1 = SUM, 2 = SUM+AVG, etc)
    pub aggregation_type: AggregationType,
    
    // Window operator config params
    pub execution_mode: WindowExecutionMode,
    pub parallelize: bool,
    pub tiling_configs: Option<Vec<Option<TileConfig>>>,
    pub lateness: Option<i64>,
    
    // Metrics collection params
    pub polling_interval_ms: u64,
    pub throughput_window_seconds: Option<u64>,
}

impl Default for WindowBenchmarkConfig {
    fn default() -> Self {
        Self {
            parallelism: 4,
            num_keys: 4,
            total_records: Some(20000),
            batch_size: 1000,
            rate: None,
            run_for_s: None,
            window_type: WindowType::Range { milliseconds: 1000 },
            num_windows: 1,
            aggregation_type: AggregationType::Retractable,
            execution_mode: WindowExecutionMode::Regular,
            parallelize: false,
            tiling_configs: None,
            lateness: None,
            polling_interval_ms: 100,
            throughput_window_seconds: Some(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub execution_time: Duration,
    pub records_produced: usize,
    pub history: PipelineStateHistory,
}

impl BenchmarkMetrics {
    pub fn new() -> Self {
        Self {
            execution_time: Duration::from_secs(0),
            records_produced: 0,
            history: PipelineStateHistory::new(),
        }
    }

    pub fn add_sample(&mut self, timestamp: u64, pipeline_state: PipelineState) {
        self.history.add_sample(timestamp, pipeline_state);
    }
}


fn build_sql_query(config: &WindowBenchmarkConfig) -> String {
    let mut select_clauses = vec![
        "event_time".to_string(),
        "key".to_string(),
        "value".to_string(),
    ];
    
    let window_def = match &config.window_type {
        WindowType::Range { milliseconds } => {
            format!("RANGE BETWEEN INTERVAL '{}' MILLISECOND PRECEDING AND CURRENT ROW", milliseconds)
        }
        WindowType::Rows { preceding } => {
            format!("ROWS BETWEEN {} PRECEDING AND CURRENT ROW", preceding)
        }
    };
    
    let window_name = "w";
    
    // Determine which aggregates to use based on aggregation_type
    let aggregates = match config.aggregation_type {
        AggregationType::Plain => {
            vec!["MIN(value) OVER {} as min_value", "MAX(value) OVER {} as max_value"]
        }
        AggregationType::Retractable => {
            vec!["SUM(value) OVER {} as sum_value", "COUNT(value) OVER {} as count_value", "AVG(value) OVER {} as avg_value"]
        }
        AggregationType::Mixed => {
            vec![
                "SUM(value) OVER {} as sum_value",
                "COUNT(value) OVER {} as count_value",
                "AVG(value) OVER {} as avg_value",
                "MIN(value) OVER {} as min_value",
                "MAX(value) OVER {} as max_value",
            ]
        }
    };
    
    // Take only the requested number of windows
    let aggregates_to_use: Vec<String> = aggregates
        .iter()
        .take(config.num_windows)
        .map(|agg| agg.replace("{}", window_name))
        .collect();
    
    select_clauses.extend(aggregates_to_use);
    
    format!(
        "SELECT {} FROM datagen_source WINDOW {} AS (PARTITION BY key ORDER BY event_time {})",
        select_clauses.join(", "),
        window_name,
        window_def
    )
}

fn update_window_configs_in_graph(
    graph: &mut crate::api::logical_graph::LogicalGraph,
    config: &WindowBenchmarkConfig,
) -> Result<()> {
    use crate::runtime::operators::operator::OperatorConfig;
    
    // Find all window operator nodes
    let window_nodes: Vec<NodeIndex> = graph
        .get_nodes()
        .filter(|node| matches!(node.operator_config, OperatorConfig::WindowConfig(_)))
        .map(|node| graph.get_node_index(&node.operator_id).unwrap())
        .collect();
    
    for node_idx in window_nodes {
        if let Some(node) = graph.get_node_by_index_mut(node_idx) {
            if let OperatorConfig::WindowConfig(ref mut window_config) = node.operator_config {
                // Update window config parameters
                window_config.execution_mode = config.execution_mode.clone();
                window_config.parallelize = config.parallelize;
                window_config.lateness = config.lateness;
                
                // Set tiling configs if provided
                if let Some(ref tiling_configs) = config.tiling_configs {
                    let num_window_exprs = window_config.window_exec.window_expr().len();
                    // Use provided tiling configs, padding with None if needed
                    let mut configs = tiling_configs.clone();
                    while configs.len() < num_window_exprs {
                        configs.push(None);
                    }
                    window_config.tiling_configs = configs.into_iter().take(num_window_exprs).collect();
                } else {
                    // Clear tiling configs
                    let num_window_exprs = window_config.window_exec.window_expr().len();
                    window_config.tiling_configs = vec![None; num_window_exprs];
                }
            }
        }
    }
    
    Ok(())
}

pub async fn run_window_benchmark(config: WindowBenchmarkConfig) -> Result<BenchmarkMetrics> {
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Create schema for datagen
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create field generators
    let fields = HashMap::from([
        // ("event_time".to_string(), FieldGenerator::ProcessingTimestamp),

        ("event_time".to_string(), FieldGenerator::IncrementalTimestamp { start_ms: 1000, step_ms: 1 }),
        ("key".to_string(), FieldGenerator::Key { 
            num_unique: config.num_keys 
        }),
        ("value".to_string(), FieldGenerator::Increment { 
            start: ScalarValue::Int64(Some(1)), 
            step: ScalarValue::Int64(Some(1))
        }),
    ]);

    let datagen_config = DatagenSourceConfig::new(
        schema.clone(),
        config.rate,
        config.total_records,
        config.run_for_s,
        config.batch_size,
        fields
    );

    // Build SQL query based on config
    let sql = build_sql_query(&config);

    println!("Query: {}", sql);

    // Create pipeline context builder
    let mut context_builder = PipelineContextBuilder::new()
        .with_parallelism(config.parallelism)
        .with_source(
            "datagen_source".to_string(), 
            SourceConfig::DatagenSourceConfig(datagen_config), 
            schema
        )
        .sql(&sql)
        .with_executor(Box::new(LocalExecutor::new()));

    
    // Set execution mode 
    if config.execution_mode == WindowExecutionMode::Request {
        // request mode has no direct sink
        context_builder = context_builder.with_execution_mode(ExecutionMode::Request);
    } else {
        context_builder = context_builder
            .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
            .with_execution_mode(ExecutionMode::Streaming);

    }
    
    let mut context = context_builder.build();
    
    // Get logical graph and update window configs
    if let Some(ref mut logical_graph) = context.get_logical_graph_mut() {
        update_window_configs_in_graph(logical_graph, &config)?;
    }

    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    let benchmark_start = Instant::now();
    let benchmark_metrics = Arc::new(Mutex::new(BenchmarkMetrics::new()));
    let benchmark_metrics_clone = benchmark_metrics.clone();
    
    let (state_updates_sender, mut state_updates_receiver) = mpsc::channel::<PipelineState>(100);
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let throughput_window_seconds_clone = config.throughput_window_seconds;
    
    // Calculate timeout duration
    let timeout_duration = if let Some(run_for_s) = config.run_for_s {
        // Add a small buffer (10%) to account for overhead
        Duration::from_secs_f64(run_for_s * 1.1)
    } else if let Some(total_records) = config.total_records {
        // Estimate timeout based on records and rate, or use a default
        if let Some(rate) = config.rate {
            Duration::from_secs_f64((total_records as f64 / rate as f64) * 1.5)
        } else {
            Duration::from_secs(300) // Default 5 minutes
        }
    } else {
        Duration::from_secs(300) // Default 5 minutes
    };
    
    // Start metrics polling task
    let state_updates_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(config.polling_interval_ms));
        let mut last_print_timestamp = Instant::now();
        
        while running_clone.load(Ordering::SeqCst) {
            interval.tick().await;
            
            // Try to receive state updates with a short timeout
            loop {
                match tokio::time::timeout(Duration::from_millis(10), state_updates_receiver.recv()).await {
                    Ok(Some(pipeline_state)) => {
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        
                        {
                            let mut metrics_guard = benchmark_metrics_clone.lock().await;
                            // Add to history
                            metrics_guard.add_sample(timestamp, pipeline_state.clone());
                        }

                        let now = Instant::now();
                        if now.duration_since(last_print_timestamp).as_secs() >= 1 {
                            println!("[{}] Worker State", timestamp);
                            
                            // Find window operator IDs
                            let window_operator_ids: Vec<String> = pipeline_state.worker_states
                                .values()
                                .flat_map(|ws| {
                                    ws.worker_metrics.as_ref()
                                        .map(|wm| {
                                            wm.operator_metrics.keys()
                                                .filter(|id| id.starts_with("Window_"))
                                                .cloned()
                                                .collect::<Vec<_>>()
                                        })
                                        .unwrap_or_default()
                                })
                                .collect();
                            
                            // Get history from metrics for throughput calculation
                            let history = {
                                let metrics_guard = benchmark_metrics_clone.lock().await;
                                metrics_guard.history.clone()
                            };
                            
                            print_pipeline_state(
                                &pipeline_state, 
                                if window_operator_ids.is_empty() { None } else { Some(&window_operator_ids) }, 
                                false, 
                                false,
                                Some(&history),
                                throughput_window_seconds_clone,
                            );
                            last_print_timestamp = now;
                        }
                    }
                    Ok(None) => {
                        // Channel closed, execution finished
                        break;
                    }
                    Err(_) => {
                        // Timeout - no more updates available right now, continue polling
                        break;
                    }
                }
            }
        }
    });
    
    // Spawn execution task
    let execution_task = tokio::spawn(async move {
        context.execute_with_state_updates(Some(state_updates_sender)).await
    });
    
    // Race between execution completion and timeout
    let timed_out = tokio::select! {
        result = execution_task => {
            // Execution finished
            match result {
                Ok(Ok(_)) => {
                    // Execution completed successfully
                    false
                }
                Ok(Err(e)) => {
                    // Execution failed
                    eprintln!("[ERROR] Execution failed: {}", e);
                    false
                }
                Err(e) => {
                    // Task panicked
                    eprintln!("[ERROR] Execution task panicked: {:?}", e);
                    false
                }
            }
        }
        _ = tokio::time::sleep(timeout_duration) => {
            // Timeout reached
            println!("[TIMEOUT] Benchmark execution timed out after {:?}", timeout_duration);
            running.store(false, Ordering::Relaxed);
            true
        }
    };
    
    // Wait a bit for metrics task to collect any remaining updates
    tokio::time::sleep(Duration::from_millis(500)).await;
    running.store(false, Ordering::Relaxed);
    
    // Wait for metrics task to finish (with a timeout to avoid hanging)
    let _ = tokio::time::timeout(Duration::from_secs(2), state_updates_task).await;
    
    let execution_time = benchmark_start.elapsed();
    
    // Log if we timed out
    if timed_out {
        println!("[WARNING] Execution was interrupted due to timeout. Collected metrics may be incomplete.");
    }
    
    // Get results
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let result_vec = client.get_vector().await.unwrap();
    storage_server.stop().await;

    // Process results
    let mut num_records_produced = 0;
    
    for (batch_idx, message) in result_vec.iter().enumerate() {
        let batch = message.record_batch();
        num_records_produced += batch.num_rows();
        println!("Batch {}: {} rows", batch_idx, batch.num_rows());
    }

    let mut benchmark_metrics = (*benchmark_metrics.lock().await).clone();
    benchmark_metrics.execution_time = execution_time;
    benchmark_metrics.records_produced = num_records_produced;

    Ok(benchmark_metrics)
}

pub fn print_benchmark_results(config: &WindowBenchmarkConfig, metrics: &BenchmarkMetrics) {
    println!("\n=== Window Operator Benchmark Results ===");
    println!("Configuration:");
    println!("  Parallelism: {}", config.parallelism);
    println!("  Number of Keys: {}", config.num_keys);
    println!("  Total Records: {:?}", config.total_records);
    println!("  Batch Size: {}", config.batch_size);
    println!("  Rate: {:?} events/sec", config.rate);
    println!("  Run Duration: {:?}s", config.run_for_s);
    println!("  Window Type: {:?}", config.window_type);
    println!("  Number of Windows: {}", config.num_windows);
    println!("  Aggregation Type: {:?}", config.aggregation_type);
    println!("  Execution Mode: {:?}", config.execution_mode);
    println!("  Parallelize: {}", config.parallelize);
    println!("  Tiling Configs: {:?}", config.tiling_configs);
    println!("  Lateness: {:?}", config.lateness);

    println!("\nResults:");
    println!("  Records Produced: {}", metrics.records_produced);
    println!("  Execution Time: {:?}", metrics.execution_time);
    if metrics.execution_time.as_secs_f64() > 0.0 {
        println!("  Average Rate: {:.2} records/sec", metrics.records_produced as f64 / metrics.execution_time.as_secs_f64());
    }
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
}

// #[tokio::test]
async fn test_window_benchmark_basic() -> Result<()> {
    let num_windows = 1;

    let mut tiling_configs = Vec::new();
    for _ in 0..num_windows {
        tiling_configs.push(Some(TileConfig::new(vec![
            TimeGranularity::Seconds(1)
        ]).unwrap()));
    }

    // TODO pass num task threads to worker via config

    let config = WindowBenchmarkConfig {
        parallelism: 4,
        num_keys: 40,
        total_records: Some(200000),
        batch_size: 1000,
        rate: None,
        run_for_s: None,
        // window_type: WindowType::Range { milliseconds: 1000 },
        window_type: WindowType::Rows { preceding: 10000 },
        num_windows: num_windows,
        aggregation_type: AggregationType::Plain,
        execution_mode: WindowExecutionMode::Request,
        parallelize: false,
        tiling_configs: Some(tiling_configs),
        lateness: None,
        polling_interval_ms: 100,
        throughput_window_seconds: Some(5),
    };

    let metrics = run_window_benchmark(config.clone()).await?;
    print_benchmark_results(&config, &metrics);

    assert_eq!(metrics.records_produced, config.total_records.unwrap());
    assert!(metrics.execution_time.as_secs_f64() > 0.0);

    Ok(())
}
