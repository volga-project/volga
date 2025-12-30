use crate::{
    api::pipeline_context::{PipelineContextBuilder, ExecutionMode},
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::{
            source::{
                datagen_source::{DatagenSourceConfig, DatagenSourceFunction, FieldGenerator},
            },
        },
        operators::{
            sink::sink_operator::SinkConfig,
            source::source_operator::SourceConfig,
        },
        tests::request_source_e2e_test::{create_test_config, run_continuous_requests},
    },
};
use datafusion::scalar::ScalarValue;
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

fn create_datagen_config(
    rate: Option<f32>,
    limit: Option<usize>,
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

    DatagenSourceConfig::new(schema, rate, limit, None, batch_size, fields)
}

fn create_payload_generator(
    parallelism: usize,
    num_unique_keys: usize,
    start_ms: i64,
    step_ms: i64,
) -> impl Fn(usize) -> serde_json::Value {
    // Generate all keys for all tasks
    let mut all_keys = Vec::new();
    for task_index in 0..parallelism {
        let task_keys = DatagenSourceFunction::gen_key_values_for_task(parallelism, task_index, num_unique_keys);
        all_keys.extend(task_keys);
    }
    
    // Track timestamps per key
    let per_key_timestamps: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    
    // Initialize timestamps for all keys
    {
        let mut timestamps = per_key_timestamps.lock().unwrap();
        for key in &all_keys {
            timestamps.insert(key.clone(), start_ms);
        }
    }
    
    move |request_id: usize| {
        // Round-robin key selection
        let key = all_keys[request_id % all_keys.len()].clone();
        
        // Get and increment timestamp for this key
        let timestamp = {
            let mut timestamps = per_key_timestamps.lock().unwrap();
            let current_ts = timestamps.get_mut(&key).unwrap();
            let ts = *current_ts;
            *current_ts += step_ms;
            // add random delta between -step_ms/2 and step_ms/2 to simulate late queries
            let delta = rand::thread_rng().gen_range(-step_ms/2..step_ms/2);
            if ts != start_ms {
                ts + delta
            } else {
                ts
            }
        };
        
        // Generate a value (using a simple formula based on request_id for determinism)

        let value = 50.0 + (request_id as f64 * 5.0);
        
        serde_json::json!({
            "data": {
                "payload": [{
                    "event_time": timestamp,
                    "key": key,
                    "value": value
                }]
            }
        })
    }
}


// TODO make single hot key test case to make sure no deadlocks occur


// TODO this test fails when running with others via 'cargo test' - why?
#[tokio::test]
async fn test_request_execution_mode() {
    let parallelism = 4;
    let max_pending_requests = 5000;
    let request_timeout_ms = 100000;
    let total_requests = 100;
    let request_source_config = create_test_config(max_pending_requests, request_timeout_ms);
    
    // Use the same rate for datagen and requests
    let rate = Some(50.0);

    // datagen config
    let num_record_to_gen = Some(100);
    let batch_size = 10;
    let start_ms = 1000;
    let step_ms = 1000;
    let num_unique_keys = 10;
    let value_start = 10.0;
    let value_step = 10.0;

    let datagen_config = create_datagen_config(rate, num_record_to_gen, batch_size, start_ms, step_ms, num_unique_keys, value_start, value_step);
    let schema = datagen_config.schema.clone();

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

    // TODO set window config - tiling, lateness, etc
    
    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_source(
            "events".to_string(),
            SourceConfig::DatagenSourceConfig(datagen_config),
            schema.clone()
        )
        .with_request_source_sink(
            SourceConfig::HttpRequestSourceConfig(request_source_config.clone()),
            Some(SinkConfig::RequestSinkConfig)
        )
        .sql(sql)
        .with_executor(Box::new(LocalExecutor::new()))
        .with_execution_mode(ExecutionMode::Request)
        .build();

    let pipeline_handle = tokio::spawn(async move {
        context.execute().await.unwrap();
    });

    sleep(Duration::from_millis(1000)).await;

    let client = reqwest::Client::new();

    println!("🚀 Sending test requests...");

    let payload_generator = create_payload_generator(parallelism, num_unique_keys, start_ms, step_ms);
    let results = run_continuous_requests(
        client,
        request_source_config.bind_address.clone(),
        rate,
        total_requests,
        Some(max_pending_requests),
        payload_generator,
    ).await;

    println!("\n📊 Test Results:");
    
    let mut successful_requests = 0;
    let mut failed_requests = 0;
    
    // Track responses per key to verify aggregate relationships
    let mut responses_by_key: HashMap<String, Vec<(String, f64, i64, f64, f64, f64)>> = HashMap::new();

    for result in &results {
        match &result.response_result {
            Ok((status, _)) => {
                if *status == 200 {
                    // Verify response structure and aggregates
                    let response_data = result.response_result.as_ref().unwrap().1
                        .get("data")
                        .and_then(|d| d.as_array());

                    match response_data {
                        Some(data_array) => {
                            if !data_array.is_empty() {
                                let first_result = &data_array[0];

                                // Verify aggregates exist
                                assert!(first_result.get("sum_value").is_some(), "Response should contain sum_value");
                                assert!(first_result.get("count_value").is_some(), "Response should contain count_value");
                                assert!(first_result.get("avg_value").is_some(), "Response should contain avg_value");
                                assert!(first_result.get("min_value").is_some(), "Response should contain min_value");
                                assert!(first_result.get("max_value").is_some(), "Response should contain max_value");
                                
                                successful_requests += 1;
                                
                                // Extract key and aggregates for tracking
                                let event_time = first_result.get("event_time").and_then(|v| v.as_str()).unwrap();
                                let key = first_result.get("key").and_then(|v| v.as_str()).unwrap().to_string();
                                let sum = first_result.get("sum_value").and_then(|v| v.as_f64()).unwrap();
                                let count = first_result.get("count_value").and_then(|v| v.as_i64()).unwrap();
                                let avg = first_result.get("avg_value").and_then(|v| v.as_f64()).unwrap();
                                let min = first_result.get("min_value").and_then(|v| v.as_f64()).unwrap();
                                let max = first_result.get("max_value").and_then(|v| v.as_f64()).unwrap();
                                
                                responses_by_key.entry(key.clone()).or_insert_with(Vec::new)
                                    .push((event_time.to_string(), sum, count, avg, min, max));
                                
                                // println!("✅ Request {} (key={}): timestamp={}, sum={}, count={}, avg={:.2}, min={}, max={}",
                                //     result.request_id, key, event_time, sum, count, avg, min, max
                                // );
                            } else {
                                println!("⚠️ Request {}: Empty response array", result.request_id);
                                failed_requests += 1;
                            }
                        }
                        None => {
                            println!("❌ Request {}: Invalid response structure", result.request_id);
                            failed_requests += 1;
                        }
                    }
                } else {
                    println!("❌ Request {}: Failed with status {}", result.request_id, status);
                    failed_requests += 1;
                }
            }
            Err(_e) => {
                // println!("❌ Request {}: Error: {}", result.request_id, e);
                failed_requests += 1;
            }
        }
    }

    println!("\n📈 Verifying aggregate relationships per key:");
    
    const EPSILON: f64 = 1e-6;
    
    // Verify aggregate relationships for each key
    for (key, responses) in &responses_by_key {
        for (event_time, sum, count, avg, min, max) in responses {
            // Verify avg * count <= sum (with tolerance for floating point precision)
            let expected_sum = avg * (*count as f64);
            assert!(
                expected_sum <= *sum + EPSILON,
                "Key {} (event_time={}): avg * count should be <= sum ({} * {} = {} <= {})",
                key, event_time, avg, count, expected_sum, sum
            );
            
            // Verify min <= max
            assert!(
                *min <= *max + EPSILON,
                "Key {} (event_time={}): min should be <= max ({} <= {})",
                key, event_time, min, max
            );
        }
        
        println!("  ✅ Key {}: {} responses verified", key, responses.len());
    }

    println!("\n  Successful: {}", successful_requests);
    println!("  Failed: {}", failed_requests);

    // assert!(successful_requests > 0, "Should have at least some successful requests");
    assert_eq!(successful_requests, total_requests, "Should complete all requests");

    pipeline_handle.abort();
}

