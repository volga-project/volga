use crate::{
    api::pipeline_context::PipelineContext,
    common::test_utils::gen_unique_grpc_port,
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator},
        operators::{sink::sink_operator::SinkConfig, source::source_operator::SourceConfig},
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use std::collections::HashMap;
use arrow::{array::Int64Array, datatypes::{DataType, Field, Schema, TimeUnit}};
use std::sync::Arc;
use std::time::Instant;
use datafusion::common::ScalarValue;

pub async fn run_window_benchmark(
    parallelism: usize,
    num_keys: usize,
    total_records: usize,
    batch_size: usize,
    rate: f32,
) -> Result<(HashMap<String, i64>, usize, std::time::Duration)> {
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Create schema for datagen
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create field generators
    let fields = HashMap::from([
        ("event_time".to_string(), FieldGenerator::ProcessingTimestamp),
        ("key".to_string(), FieldGenerator::Key { 
            num_unique: num_keys 
        }),
        ("value".to_string(), FieldGenerator::Increment { 
            start: ScalarValue::Int64(Some(1)), 
            step: ScalarValue::Int64(Some(1))
        }),
    ]);

    let datagen_config = DatagenSourceConfig::new(
        schema.clone(),
        Some(rate),
        Some(total_records),
        None,
        batch_size,
        fields
    );

    // Create streaming context with datagen source and tumbling window
    let context = PipelineContext::new()
        .with_parallelism(parallelism)
        .with_source(
            "datagen_source".to_string(), 
            SourceConfig::DatagenSourceConfig(datagen_config), 
            schema
        )
        .with_sink(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))
        .sql("
            SELECT 
                event_time, 
                key, 
                value, 
                SUM(value) OVER w as sum_value
            FROM datagen_source
            WINDOW w AS (
                PARTITION BY key 
                ORDER BY event_time 
                RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
            )
        ")
        .with_executor(Box::new(LocalExecutor::new()));

    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    let start_time = Instant::now();
    
    // Execute the pipeline job
    context.execute().await.unwrap();
    
    let execution_time = start_time.elapsed();
    
    // Get results
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let result_vec = client.get_vector().await.unwrap();
    storage_server.stop().await;

    // Process results
    let mut key_sums = HashMap::new();
    let mut num_records_produced = 0;
    
    for (batch_idx, message) in result_vec.iter().enumerate() {
        let batch = message.record_batch();
        // println!("produced batch: {:?}", batch);
        num_records_produced += batch.num_rows();
        
        if batch.num_columns() != 4 {
            println!("Warning: Batch {} has {} columns instead of 4", batch_idx, batch.num_columns());
            continue;
        }
        
        // Columns: event_time, key, value, sum_value
        let key_column = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let sum_column = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            let key = key_column.value(i).to_string();
            let sum = sum_column.value(i);
            
            // For window functions, we want the final sum value per key (not accumulating)
            key_sums.insert(key, sum);
        }
        
        println!("Batch {}: {} rows", batch_idx, batch.num_rows());
    }

    Ok((key_sums, num_records_produced, execution_time))
}

#[tokio::test]
async fn test_window_benchmark() -> Result<()> {
    let parallelism = 1;
    let num_keys = 4;
    let total_records = 20;
    let batch_size = 1;
    let rate = 10.0;

    let (key_sums, num_records_produced, execution_time) = run_window_benchmark(
        parallelism,
        num_keys,
        total_records,
        batch_size,
        rate
    ).await?;

    println!("\n=== Window Operator Benchmark Results ===");
    println!("Configuration:");
    println!("  Parallelism: {}", parallelism);
    println!("  Number of Keys: {}", num_keys);
    println!("  Total Records: {}", total_records);
    println!("  Batch Size: {}", batch_size);
    println!("  Rate: {} events/sec", rate);

    println!("\nResults:");
    println!("  Records Produced: {}", num_records_produced);
    println!("  Execution Time: {:?}", execution_time);
    println!("  Average Rate: {}", num_records_produced as f64 / execution_time.as_secs_f64());
    println!("  Unique Keys Processed: {}", key_sums.len());
    println!("  Expected Keys: {}", num_keys);
    
    println!("\nKey Sums:");
    for (key, sum) in &key_sums {
        println!("  {}: {}", key, sum);
    }
    
    println!("==========================================\n");

    // Basic assertions
    assert_eq!(key_sums.len(), num_keys, 
        "Should have exactly {} unique keys", num_keys);
    
    // Verify all sums are positive
    for (key, sum) in &key_sums {
        assert!(*sum > 0, "Sum for key {} should be positive: {}", key, sum);
    }

    // For window functions, each record contributes to multiple window calculations
    // We can't easily verify the exact total sum since it depends on the window overlap
    // Instead, verify that we have reasonable sums for each key
    let total_sum: i64 = key_sums.values().sum();
    assert!(total_sum > 0, "Total sum should be positive: {}", total_sum);

    Ok(())
}

