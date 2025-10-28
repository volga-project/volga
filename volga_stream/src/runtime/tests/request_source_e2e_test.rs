use crate::{
    api::{logical_graph::LogicalGraph, pipeline_context::PipelineContext, planner::{Planner, PlanningContext}},
    common::test_utils::IdentityMapFunction,
    executor::local_executor::LocalExecutor,
    runtime::{
        functions::{
            key_by::{KeyByFunction, key_by_function::extract_datafusion_window_exec},
            map::MapFunction,
            source::request_source::RequestSourceConfig,
        },
        operators::{
            operator::OperatorConfig,
            sink::sink_operator::SinkConfig,
            source::source_operator::SourceConfig,
        },
    },
};
use datafusion::prelude::SessionContext;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use tokio::time::{sleep, Duration, interval, Instant};
use futures::future::join_all;
use tokio::sync::Semaphore;
use rand;
use std::collections::HashMap;

fn create_test_config(max_pending_requests: usize, request_timeout_ms: u64) -> RequestSourceConfig {
    // Use a random high port to avoid conflicts
    let port = 8000 + (rand::random::<u16>() % 1000);
    
    RequestSourceConfig::new(
        format!("127.0.0.1:{}", port),
        max_pending_requests,
        request_timeout_ms,
    )
}

#[derive(Debug, Clone)]
struct RequestResult {
    request_id: usize,
    request_payload: serde_json::Value,
    response_result: Result<(u16, serde_json::Value), String>,
    duration: Duration,
}

/// Generate a random request payload for testing
fn generate_request_payload(_request_id: usize) -> serde_json::Value {
    use rand::Rng;
    
    let names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"];
    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"];
    
    let mut rng = rand::thread_rng();
    let num_records = rng.gen_range(1..=4); // 1 to 4 records per request
    
    let mut records = Vec::new();
    for _ in 0..num_records {
        let name = names[rng.gen_range(0..names.len())];
        let department = departments[rng.gen_range(0..departments.len())];
        let salary = rng.gen_range(45000..=120000); // Random salary between 45k and 120k
        
        records.push(serde_json::json!({
            "name": name,
            "department": department,
            "salary": salary
        }));
    }
    
    serde_json::json!({
        "data": {
            "payload": records
        }
    })
}

/// Helper function to continuously run requests at a specified rate with concurrency control
async fn run_continuous_requests(
    client: reqwest::Client,
    bind_address: String,
    requests_per_second: f64,
    total_requests: usize,
    max_concurrent: Option<usize>,
) -> Vec<RequestResult> {
    let semaphore = max_concurrent.map(|limit| Arc::new(Semaphore::new(limit)));
    let mut results = Vec::new();
    let mut interval = interval(Duration::from_millis((1000.0 / requests_per_second) as u64));
    
    let mut request_futures = Vec::new();
    
    for i in 0..total_requests {
        // Wait for the next tick to maintain the desired rate
        interval.tick().await;
        
        let request_payload = generate_request_payload(i);
        let client = client.clone();
        let bind_address = bind_address.clone();
        let semaphore = semaphore.clone();
        
        let future = tokio::spawn(async move {
            // Acquire semaphore permit if concurrency limit is set
            let _permit = if let Some(sem) = &semaphore {
                Some(sem.acquire().await.unwrap())
            } else {
                None
            };
            
            let start_time = Instant::now();
            let response_result = client
                .post(&format!("http://{}/request", bind_address))
                .json(&request_payload)
                .timeout(Duration::from_secs(10))
                .send()
                .await;
            
            let duration = start_time.elapsed();
            
            let result = match response_result {
                Ok(response) => {
                    let status = response.status().as_u16();
                    match response.json::<serde_json::Value>().await {
                        Ok(body) => Ok((status, body)),
                        Err(e) => Err(format!("Failed to parse response JSON: {}", e)),
                    }
                }
                Err(e) => Err(format!("Request failed: {}", e)),
            };
            
            RequestResult {
                request_id: i,
                request_payload,
                response_result: result,
                duration,
            }
        });
        
        request_futures.push(future);
    }
    
    // Wait for all requests to complete
    let future_results = join_all(request_futures).await;
    
    for future_result in future_results {
        match future_result {
            Ok(request_result) => results.push(request_result),
            Err(e) => eprintln!("Request task failed: {}", e),
        }
    }
    
    results
}

/// Verify that request and response payloads match
fn verify_request_response_match(request_result: &RequestResult) -> Result<(), String> {
    match &request_result.response_result {
        Ok((status, response_body)) => {
            if *status != 200 {
                return Err(format!("Request {} failed with status: {}", request_result.request_id, status));
            }
            
            // Extract request payload data
            let request_data = request_result.request_payload
                .get("data")
                .and_then(|d| d.get("payload"))
                .and_then(|p| p.as_array())
                .ok_or_else(|| format!("Request {} has invalid payload structure", request_result.request_id))?;
            
            // Extract response data
            let response_data = response_body
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| format!("Request {} response has invalid structure", request_result.request_id))?;
            
            // Verify same number of records
            if request_data.len() != response_data.len() {
                return Err(format!(
                    "Request {} record count mismatch: request has {}, response has {}",
                    request_result.request_id, request_data.len(), response_data.len()
                ));
            }
            
            // Create maps for comparison (since KeyBy might reorder records)
            let mut request_records: HashMap<String, serde_json::Value> = HashMap::new();
            let mut response_records: HashMap<String, serde_json::Value> = HashMap::new();
            
            // Index request records by a composite key (name + department + salary)
            for record in request_data {
                let key = format!("{}:{}:{}", 
                    record.get("name").unwrap().as_str().unwrap(),
                    record.get("department").unwrap().as_str().unwrap(),
                    record.get("salary").unwrap().as_i64().unwrap()
                );
                request_records.insert(key, record.clone());
            }
            
            // Index response records by the same composite key
            for record in response_data {
                let key = format!("{}:{}:{}", 
                    record.get("name").unwrap().as_str().unwrap(),
                    record.get("department").unwrap().as_str().unwrap(),
                    record.get("salary").unwrap().as_i64().unwrap()
                );
                response_records.insert(key, record.clone());
            }
            
            // Verify all request records are present in response
            for (key, request_record) in &request_records {
                let response_record = response_records.get(key)
                    .ok_or_else(|| format!("Request {} missing record in response: {}", request_result.request_id, key))?;
                
                // Verify field values match
                for field in ["name", "department", "salary"] {
                    let request_value = request_record.get(field).unwrap();
                    let response_value = response_record.get(field).unwrap();
                    
                    if request_value != response_value {
                        return Err(format!(
                            "Request {} field '{}' mismatch: request={}, response={}",
                            request_result.request_id, field, request_value, response_value
                        ));
                    }
                }
            }
            
            Ok(())
        }
        Err(error_msg) => Err(format!("Request {} failed: {}", request_result.request_id, error_msg)),
    }
}

#[tokio::test]
async fn test_request_source_sink_e2e() {
    // Operator config
    let max_pending_requests = 100;
    let request_timeout_ms = 5000;

    // Test params
    let requests_per_second = 100.0;
    let total_requests = 400;

    // Create test configuration
    let config = create_test_config(max_pending_requests, request_timeout_ms);
    let bind_address = config.bind_address.clone();

    // Create schema that matches our test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int64, false),
    ]));

    // Create DataFusion planner to extract window exec
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    
    // Register a dummy source with our schema
    planner.register_source(
        "employees".to_string(),
        SourceConfig::VectorSourceConfig(crate::runtime::operators::source::source_operator::VectorSourceConfig::new(vec![])),
        schema.clone()
    );

    // Extract window exec from SQL query
    let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY name, department ORDER BY salary) as rn FROM employees";
    let window_exec = extract_datafusion_window_exec(sql, &mut planner).await
        .expect("Should extract window exec from SQL");

    // Create pipeline operators
    let parallelism = 4; // Test with parallelism > 1
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::HttpRequestSourceConfig(config)),
        OperatorConfig::KeyByConfig(KeyByFunction::DataFusion(
            crate::runtime::functions::key_by::key_by_function::DataFusionKeyFunction::new_window(window_exec)
        )),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::RequestSinkConfig),
    ];

    // Create logical graph, no chaining
    let logical_graph = LogicalGraph::from_linear_operators(operators, parallelism, false);

    // Create pipeline context with LocalExecutor
    let context = PipelineContext::new()
        .with_parallelism(parallelism)
        .with_logical_graph(logical_graph)
        .with_executor(Box::new(LocalExecutor::new()));

    // Start pipeline execution in background
    // TODO implement stop for context
    let pipeline_handle = tokio::spawn(async move {
            context.execute().await.unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_millis(200)).await;

    // Create test client
    let client = reqwest::Client::new();

    println!("🚀 Starting continuous request load test...");
    
    
    // Run continuous requests using the helper function
    let results = run_continuous_requests(
        client.clone(),
        bind_address.clone(),
        requests_per_second,
        total_requests,
        Some(max_pending_requests),
    ).await;

    println!("📊 Completed {} requests, analyzing results...", results.len());

    // Analyze results
    let mut successful_requests = 0;
    let mut failed_requests = 0;
    let mut all_durations = Vec::new();
    let mut payload_match_errors = Vec::new();

    for result in &results {
        all_durations.push(result.duration);
        
        match verify_request_response_match(result) {
            Ok(()) => {
                successful_requests += 1;
            }
            Err(error) => {
                failed_requests += 1;
                payload_match_errors.push(error.clone());
                println!("❌ Request {} failed: {}", result.request_id, error);
            }
        }
    }

    // Calculate duration statistics using statistical crate
    let duration_ms: Vec<f64> = all_durations.iter()
        .map(|d| d.as_secs_f64() * 1000.0) // Convert to milliseconds
        .collect();
    
    let total_duration: Duration = all_durations.iter().sum();
    
    let (avg_duration, p50_duration, p95_duration, p99_duration, min_duration, max_duration, std_dev_ms) = if !duration_ms.is_empty() {
        // Calculate statistics manually with proper algorithms
        let mut sorted_durations = duration_ms.clone();
        sorted_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let len = sorted_durations.len();
        let sum: f64 = sorted_durations.iter().sum();
        let avg_ms = sum / len as f64;
        
        // Calculate standard deviation
        let variance = sorted_durations.iter()
            .map(|x| (x - avg_ms).powi(2))
            .sum::<f64>() / len as f64;
        let std_dev = variance.sqrt();
        
        let min_ms = sorted_durations[0];
        let max_ms = sorted_durations[len - 1];
        
        // Calculate percentiles using proper interpolation
        let percentile = |p: f64| -> f64 {
            let index = p * (len - 1) as f64;
            let lower = index.floor() as usize;
            let upper = index.ceil() as usize;
            
            if lower == upper {
                sorted_durations[lower]
            } else {
                let weight = index - lower as f64;
                sorted_durations[lower] * (1.0 - weight) + sorted_durations[upper] * weight
            }
        };
        
        let p50_ms = percentile(0.5);
        let p95_ms = percentile(0.95);
        let p99_ms = percentile(0.99);
        
        (
            Duration::from_secs_f64(avg_ms / 1000.0),
            Duration::from_secs_f64(p50_ms / 1000.0),
            Duration::from_secs_f64(p95_ms / 1000.0),
            Duration::from_secs_f64(p99_ms / 1000.0),
            Duration::from_secs_f64(min_ms / 1000.0),
            Duration::from_secs_f64(max_ms / 1000.0),
            std_dev,
        )
    } else {
        (Duration::from_secs(0), Duration::from_secs(0), Duration::from_secs(0), Duration::from_secs(0), Duration::from_secs(0), Duration::from_secs(0), 0.0)
    };

    // Print summary statistics
    println!("\n📈 Test Results Summary:");
    println!("  Total Requests: {}", results.len());
    println!("  Successful: {}", successful_requests);
    println!("  Failed: {}", failed_requests);
    println!("  Success Rate: {:.1}%", (successful_requests as f64 / results.len() as f64) * 100.0);
    println!("  Response Time Stats:");
    println!("    Average: {:?}", avg_duration);
    println!("    Median (P50): {:?}", p50_duration);
    println!("    P95: {:?}", p95_duration);
    println!("    P99: {:?}", p99_duration);
    println!("    Min: {:?}", min_duration);
    println!("    Max: {:?}", max_duration);
    println!("    Std Dev: {:.2}ms", std_dev_ms);
    println!("  Total Test Duration: {:?}", total_duration);
    println!("  Actual RPS: {:.2}", results.len() as f64 / total_duration.as_secs_f64());

    // Verify test requirements
    assert_eq!(results.len(), total_requests, "Should complete all requested requests");
    assert_eq!(successful_requests, total_requests, "Should have at least some successful requests");
    
    // TODO - implement stop for context and stop the pipeline
}
