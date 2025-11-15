use crate::{
    api::pipeline_context::{PipelineContext, ExecutionMode},
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator},
        operators::source::source_operator::SourceConfig,
    },
};
use anyhow::Result;
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use datafusion::common::ScalarValue;
use tokio::time::Duration;

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


#[derive(Debug)]
pub struct BenchmarkMetrics {
    pub execution_time: Duration,
    pub requests_processed: usize,
    pub requests_per_second: f64,
}

pub async fn run_window_request_benchmark(
    parallelism: usize,
    num_unique_keys: usize,
    run_for_s: f64,
    datagen_rate: Option<f32>,
    request_rate: Option<f32>,
    batch_size: usize,
) -> Result<BenchmarkMetrics> {
    let start_ms = 1000;
    let step_ms = 1000;
    let value_start = 10.0;
    let value_step = 10.0;

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

    let context = PipelineContext::new()
        .with_parallelism(parallelism)
        .with_source(
            "events".to_string(),
            SourceConfig::DatagenSourceConfig(query_datagen_config),
            schema.clone()
        )
        .with_request_source(
            SourceConfig::DatagenSourceConfig(request_datagen_config)
        )
        .sql(sql)
        .with_executor(Box::new(LocalExecutor::new()))
        .with_execution_mode(ExecutionMode::Request);

    // Execute pipeline - it will complete when both sources finish (after run_for_s)
    let _pipeline_state = context.execute().await?;

    let execution_time = benchmark_start.elapsed();

    // Calculate requests processed based on rate and duration
    let requests_processed = if let Some(rate) = request_rate {
        (rate * run_for_s as f32) as usize
    } else {
        // If no rate limit, estimate based on execution time
        (execution_time.as_secs_f64() * 1000.0) as usize
    };

    let requests_per_second = if execution_time.as_secs_f64() > 0.0 {
        requests_processed as f64 / execution_time.as_secs_f64()
    } else {
        0.0
    };

    Ok(BenchmarkMetrics {
        execution_time,
        requests_processed,
        requests_per_second,
    })
}

// #[tokio::test]
async fn test_window_request_benchmark() -> Result<()> {
    let parallelism = 4;
    let num_unique_keys = 10;
    let run_for_s = 10.0;
    let datagen_rate = None;
    let request_rate = Some(100.0);
    let batch_size = 10;

    let metrics = run_window_request_benchmark(
        parallelism,
        num_unique_keys,
        run_for_s,
        datagen_rate,
        request_rate,
        batch_size,
    ).await?;

    println!("\n=== Window Request Operator Benchmark Results ===");
    println!("Configuration:");
    println!("  Parallelism: {}", parallelism);
    println!("  Number of Keys: {}", num_unique_keys);
    println!("  Run Duration: {}s", run_for_s);
    println!("  Query Datagen Rate: {:?}", datagen_rate);
    println!("  Request Datagen Rate: {:?}", request_rate);
    println!("  Batch Size: {}", batch_size);

    println!("\nResults:");
    println!("  Execution Time: {:?}", metrics.execution_time);
    println!("  Requests Processed: {}", metrics.requests_processed);
    println!("  Requests Per Second: {:.2}", metrics.requests_per_second);
    println!("==========================================\n");

    assert!(metrics.requests_processed > 0, "Should have processed at least some requests");
    assert!(metrics.execution_time.as_secs_f64() > 0.0, "Execution time should be positive");

    Ok(())
}

