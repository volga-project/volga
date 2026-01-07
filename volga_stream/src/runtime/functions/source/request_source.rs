use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use anyhow::Result;
use std::sync::Arc;
use std::any::Any;
use tokio::sync::{mpsc, oneshot, Mutex};
use dashmap::DashMap;
use tokio::time::{timeout, Duration};
use axum::{
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use super::json_utils::{record_batch_to_json, json_to_record_batch};
use arrow::compute::concat_batches;
use std::collections::HashMap;
use uuid::Uuid;

use crate::common::message::Message;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use super::source_function::SourceFunctionTrait;

/// Reserved metadata field names
pub const SOURCE_TASK_INDEX_FIELD: &str = "_source_task_index";
pub const SOURCE_REQUEST_ID_FIELD: &str = "_source_request_id";

/// Configuration for request source
#[derive(Debug, Clone)]
pub struct RequestSourceConfig {
    pub bind_address: String,
    pub max_pending_requests: usize,
    pub request_timeout_ms: u64,
    pub schema: Option<SchemaRef>,
}

impl RequestSourceConfig {
    pub fn new(bind_address: String, max_pending_requests: usize, request_timeout_ms: u64) -> Self {
        Self {
            bind_address,
            max_pending_requests,
            request_timeout_ms,
            schema: None,
        }
    }
    
    pub fn set_schema(mut self, schema: arrow::datatypes::SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Request payload structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPayload {
    pub data: serde_json::Value,
}

/// Response payload structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsePayload {
    pub data: serde_json::Value,
}

/// Internal request structure 
#[derive(Debug)]
pub struct PendingRequest {
    pub request_id: String,
    pub payload: RequestPayload,
}

/// Structure to track accumulated response data for a pending request
#[derive(Debug)]
struct PendingResponse {
    pub response_tx: oneshot::Sender<ResponsePayload>,
    pub expected_record_count: usize,
    pub record_batches: Vec<RecordBatch>,
    pub record_count: usize,
}

/// Shared request source processor - one per worker
#[derive(Debug)]
pub struct RequestSourceProcessor {
    config: RequestSourceConfig,
    
    // HTTP server handle
    server_handle: Option<tokio::task::JoinHandle<()>>,
    
    // Response processing task handle
    response_processor_handle: Option<tokio::task::JoinHandle<()>>,
    
    // Shared request queue that all source tasks read from
    request_tx: mpsc::Sender<PendingRequest>,
    request_rx: Arc<Mutex<mpsc::Receiver<PendingRequest>>>,
    
    // Channel for receiving processed responses from sink tasks
    response_rx: Option<mpsc::Receiver<Message>>,
    response_tx: mpsc::Sender<Message>,
    
    // Map to track pending requests by request_id
    pending_requests: Arc<DashMap<String, PendingResponse>>,
}


pub fn extract_request_source_config(graph: &ExecutionGraph) -> Option<RequestSourceConfig> {
    for (_vertex_id, vertex) in graph.get_vertices() {
        if let OperatorConfig::SourceConfig(SourceConfig::HttpRequestSourceConfig(config)) = &vertex.operator_config {
            return Some(config.clone());
        }
    }
    None
}

impl RequestSourceProcessor {
    pub fn new(config: RequestSourceConfig) -> Self {
        let (request_tx, request_rx) = mpsc::channel(config.max_pending_requests);
        let (response_tx, response_rx) = mpsc::channel(config.max_pending_requests);
        
        Self {
            config,
            server_handle: None,
            response_processor_handle: None,
            request_tx,
            request_rx: Arc::new(Mutex::new(request_rx)),
            response_rx: Some(response_rx),
            response_tx,
            pending_requests: Arc::new(DashMap::new()),
        }
    }
    
    /// Get a shared receiver handle for source tasks to read requests from
    pub fn get_shared_request_receiver(&self) -> Arc<Mutex<mpsc::Receiver<PendingRequest>>> {
        self.request_rx.clone()
    }
    
    /// Get the response sender for sink tasks to send processed responses
    pub fn get_response_sender(&self) -> mpsc::Sender<Message> {
        self.response_tx.clone()
    }
    
    /// Get the number of pending requests
    pub fn get_pending_count(&self) -> usize {
        self.pending_requests.len()
    }
    
    /// Start the HTTP server and response processor
    pub async fn start(&mut self) -> Result<()> {
        self.start_server().await?;
        self.start_response_processor()?;
        Ok(())
    }
    
    /// Stop the HTTP server and response processor
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_server().await?;
        self.stop_response_processor().await?;
        Ok(())
    }
    
    async fn start_server(&mut self) -> Result<()> {
        let bind_address = self.config.bind_address.clone();
        let max_pending = self.config.max_pending_requests;
        let request_timeout_ms = self.config.request_timeout_ms;
        let pending_requests = self.pending_requests.clone();
        let request_tx = self.request_tx.clone();
        
        let app = Router::new()
            .route("/request", post({
                let pending_requests = pending_requests.clone();
                move |payload| handle_request(request_tx.clone(), max_pending, request_timeout_ms, pending_requests.clone(), payload)
            }));
        
        self.server_handle = Some(tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(&bind_address).await.expect("Failed to bind to address");
            axum::serve(listener, app)
                .await
                .expect("Server failed");
        }));
        
        Ok(())
    }
    
    async fn stop_server(&mut self) -> Result<()> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
        Ok(())
    }
    
    fn start_response_processor(&mut self) -> Result<()> {
        let response_rx = self.response_rx.take()
            .ok_or_else(|| anyhow::anyhow!("Response receiver already taken"))?;
        let pending_requests = self.pending_requests.clone();
        
        self.response_processor_handle = Some(tokio::spawn(async move {
            Self::response_processor_task(response_rx, pending_requests).await;
        }));
        
        Ok(())
    }
    
    async fn stop_response_processor(&mut self) -> Result<()> {
        if let Some(handle) = self.response_processor_handle.take() {
            handle.abort();
        }
        Ok(())
    }
    
    async fn response_processor_task(
        mut response_rx: mpsc::Receiver<Message>,
        pending_requests: Arc<DashMap<String, PendingResponse>>,
    ) {
        while let Some(response) = response_rx.recv().await {
            if let Err(e) = Self::process_single_response(response, &pending_requests).await {
                eprintln!("Error processing response: {}", e);
            }
        }
    }
    
    async fn process_single_response(
        response: Message,
        pending_requests: &Arc<DashMap<String, PendingResponse>>,
    ) -> Result<()> {
        let request_id = response.get_extras().and_then(|extras| extras.get(SOURCE_REQUEST_ID_FIELD).cloned())
            .ok_or_else(|| anyhow::anyhow!("Response missing {} in extras", SOURCE_REQUEST_ID_FIELD))?;
        
        let record_batch = response.record_batch().clone();
        let record_count = record_batch.num_rows();
        
        let mut should_send_response = false;
        // Check if we have a pending request for this ID
        if let Some(mut pending_entry) = pending_requests.get_mut(&request_id) {
            // Accumulate the record batch
            pending_entry.record_batches.push(record_batch);
            pending_entry.record_count += record_count;
            if pending_entry.record_count > pending_entry.expected_record_count {
                panic!("Received more records than expected for request {}", request_id);
            }
            if pending_entry.record_count == pending_entry.expected_record_count {
                should_send_response = true;
            }
        }
            // Check if we have received all expected records
        if should_send_response {
            // We have all the data, now combine all record batches and send response
            let (_, pending_response) = pending_requests.remove(&request_id).expect("Pending request not found");
            let response_tx = pending_response.response_tx;
            let batches = pending_response.record_batches;
            let schema = batches[0].schema();
            let result_batch = concat_batches(&schema, &batches).expect("Failed to concatenate record batches");
            let response_data = record_batch_to_json(&result_batch).expect("Failed to convert record batch to JSON");

            let response_payload = ResponsePayload {
                data: response_data,
            };
            
            let _ = response_tx.send(response_payload);
        }
        
        Ok(())
    }
}

/// HTTP-based request source function - now uses shared processor
#[derive(Debug)]
pub struct HttpRequestSourceFunction {
    runtime_context: Option<RuntimeContext>,
    
    // Shared request receiver from the processor
    shared_request_rx: Option<Arc<tokio::sync::Mutex<mpsc::Receiver<PendingRequest>>>>,
    
    // Schema for converting JSON to RecordBatch
    schema: SchemaRef,
}

impl HttpRequestSourceFunction {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            runtime_context: None,
            shared_request_rx: None,
            schema: schema,
        }
    }
    
    fn create_message_from_request(&self, request_id: &str, payload: &RequestPayload) -> Result<Message> {
        let task_index = self.runtime_context.as_ref().expect("Runtime context not set").task_index();
        
        let payload_array = match &payload.data {
            serde_json::Value::Object(obj) => {
                match obj.get("payload") {
                    Some(serde_json::Value::Array(arr)) => serde_json::Value::Array(arr.clone()),
                    Some(_) => return Err(anyhow::anyhow!("Expected 'payload' field to be an array")),
                    None => return Err(anyhow::anyhow!("Missing 'payload' field in request data")),
                }
            }
            _ => return Err(anyhow::anyhow!("Expected request data to be an object with 'payload' field")),
        };
        
        let record_batch = json_to_record_batch(&payload_array, self.schema.clone())?;
        
        let mut extras = HashMap::new();
        extras.insert(SOURCE_TASK_INDEX_FIELD.to_string(), task_index.to_string());
        extras.insert(SOURCE_REQUEST_ID_FIELD.to_string(), request_id.to_string());
        
        let message = Message::new(
            None, // upstream_vertex_id - will be set by the runtime
            record_batch,
            None, // ingest_timestamp - will be set by the runtime
            Some(extras)
        );
        
        Ok(message)
    }
}

async fn handle_request(
    request_tx: mpsc::Sender<PendingRequest>,
    max_pending_requests: usize,
    request_timeout_ms: u64,
    pending_requests: Arc<DashMap<String, PendingResponse>>,
    Json(payload): Json<RequestPayload>,
) -> Result<Json<ResponsePayload>, StatusCode> {
    if pending_requests.len() >= max_pending_requests {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    
    let request_id = Uuid::new_v4().to_string();
    
    // Calculate expected record count from the request payload
    let expected_record_count = match &payload.data {
        serde_json::Value::Object(obj) => {
            if let Some(serde_json::Value::Array(payload_array)) = obj.get("payload") {
                payload_array.len()
            } else {
                return Err(StatusCode::BAD_REQUEST);
            }
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    
    let (response_tx, response_rx) = oneshot::channel();
    
    // Store pending request with expected record count
    let pending_response = PendingResponse {
        response_tx,
        expected_record_count,
        record_batches: Vec::new(),
        record_count: 0,
    };
    
    pending_requests.insert(request_id.clone(), pending_response);
    let pending_request = PendingRequest {
        request_id: request_id.clone(),
        payload,
    };
    
    // send to fetch to process
    if request_tx.send(pending_request).await.is_err() {
        pending_requests.remove(&request_id);
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    
    match timeout(Duration::from_millis(request_timeout_ms), response_rx).await {
        Ok(Ok(response)) => Ok(Json(response)),
        Ok(Err(_)) => {
            // Remove from pending requests on error (if not already removed)
            pending_requests.remove(&request_id);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(_) => {
            // Timeout - remove from pending requests
            pending_requests.remove(&request_id);
            Err(StatusCode::REQUEST_TIMEOUT)
        }
    }
}

#[async_trait]
impl FunctionTrait for HttpRequestSourceFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        if self.shared_request_rx.is_some() {
            panic!("shared_request_rx is already set")
        }
        self.shared_request_rx = Some(context.get_request_sink_source_request_receiver().expect("request_sink_source_request_receiver should be set"));
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[async_trait]
impl SourceFunctionTrait for HttpRequestSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        if let Some(shared_rx) = &self.shared_request_rx {
            // Compete with other tasks for messages from the shared queue
            let pending_request = {
                let mut rx = shared_rx.lock().await;
                rx.recv().await
            };
            
            if let Some(pending_request) = pending_request {
                match self.create_message_from_request(&pending_request.request_id, &pending_request.payload) {
                    Ok(message) => {
                        return Some(message);
                    }
                    Err(e) => {
                        eprintln!("Error creating message from request: {}", e);
                    }
                }
            }
        }
        panic!("No shared_request_rx set");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::runtime_context::RuntimeContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use tokio::time::{sleep, Duration};
    use futures::FutureExt;
    use rand;
    use std::sync::Arc;

    
    fn create_test_config(schema: SchemaRef) -> RequestSourceConfig {
        // Use a random high port to avoid conflicts
        let port = 8000 + (rand::random::<u16>() % 1000);
        
        let mut config = RequestSourceConfig::new(
            format!("127.0.0.1:{}", port),
            2, // max_pending_requests
            1000, // request_timeout_ms
        );
        config.schema = Some(schema);
        config
    }

    fn create_test_runtime_context() -> RuntimeContext {
        RuntimeContext::new(Arc::<str>::from("test_vertex"), 0, 1, None, None, None)
    }
    
    async fn create_test_processor_and_source(schema: SchemaRef) -> (RequestSourceProcessor, HttpRequestSourceFunction) {
        let config = create_test_config(schema.clone());
        let mut processor = RequestSourceProcessor::new(config);
        
        // Start the processor
        processor.start().await.unwrap();
        
        let source = HttpRequestSourceFunction::new(schema.clone());
        
        (processor, source)
    }

    #[tokio::test]
    async fn test_request_fetch_response() {

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let (mut processor, mut source) = create_test_processor_and_source(schema.clone()).await;
        let mut runtime_context = create_test_runtime_context();
        runtime_context.set_request_sink_source_response_sender(processor.get_response_sender());
        runtime_context.set_request_sink_source_request_receiver(processor.get_shared_request_receiver().clone());
        
        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        let bind_address = processor.config.bind_address.clone();
        
        // Wait a bit for server to start
        sleep(Duration::from_millis(50)).await;
        
        // Verify initial state
        assert_eq!(processor.get_pending_count(), 0);
        
        let client = reqwest::Client::new();
        let test_payload = serde_json::json!({
            "data": {
                "payload": [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30}
                ]
            }
        });
        
        println!("Sending request...");
        // Send HTTP request in background (don't await yet)
        let request_handle = tokio::spawn({
            let client = client.clone();
            let bind_address = bind_address.clone();
            let test_payload = test_payload.clone();
            async move {
                client
                    .post(&format!("http://{}/request", bind_address))
                    .json(&test_payload)
                    .send()
                    .await
            }
        });
        
        // Wait a bit for request to arrive and be processed by HTTP handler
        sleep(Duration::from_millis(50)).await;
        
        // Verify that the request is now pending (in DashMap, waiting for response)
        assert_eq!(processor.get_pending_count(), 1);
        
        println!("Fetching message...");
        // Call fetch() to get the message from the HTTP request
        let message = source.fetch().await.expect("Should receive a message from HTTP request");
        
        println!("Message fetched");
        // Verify the message structure and content
        let record_batch = message.record_batch();
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);
        
        // Verify extras contain correct metadata
        let extras = message.get_extras().unwrap();
        assert_eq!(extras.get(SOURCE_TASK_INDEX_FIELD).unwrap(), "0"); // parallelism = 1
        let request_id = extras.get(SOURCE_REQUEST_ID_FIELD).unwrap();
        assert!(!request_id.is_empty()); // Request ID should be generated
        
        // Verify record batch data is correctly parsed from JSON
        let schema = record_batch.schema();
        
        let name_column_index = schema.column_with_name("name").expect("Should have name column").0;
        let name_column = record_batch.column(name_column_index).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        
        let age_column_index = schema.column_with_name("age").expect("Should have age column").0;
        let age_column = record_batch.column(age_column_index).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(age_column.value(0), 25);
        assert_eq!(age_column.value(1), 30);
        
        // Verify internal state: request should still be pending (waiting for response)
        assert_eq!(processor.get_pending_count(), 1);
        assert!(processor.pending_requests.contains_key(request_id));
        
        // Simulate processing by creating separate response messages with the same extras for each record
        for i in 0..record_batch.num_rows() {
            let record_batch_slice = record_batch.slice(i, 1);
            let response_message = Message::new(
                Some("processor".to_string()),
                record_batch_slice,
                Some(12345),
                Some(extras.clone())
            );
        
            // Send response back through the processor's response channel
            let response_tx = processor.get_response_sender();
            response_tx.send(response_message).await.unwrap();
        }

        // Wait a bit for response processing
        sleep(Duration::from_millis(50)).await;
        
        // Verify internal state: pending request should be removed after response processing
        assert!(!processor.pending_requests.contains_key(request_id));
        assert_eq!(processor.get_pending_count(), 0);
        
        println!("Waiting for request to complete...");
        // Verify the HTTP request completes successfully
        let response = request_handle.await.unwrap().unwrap();
        
        println!("Request completed");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        
        // Verify response body contains the processed data
        let response_body: serde_json::Value = response.json().await.unwrap();
        assert!(response_body.get("data").is_some());
        let response_data = response_body.get("data").unwrap();
        assert!(response_data.is_array());
        
        // Verify the response contains the original data
        let response_array = response_data.as_array().unwrap();
        assert_eq!(response_array.len(), 2);
        assert_eq!(response_array[0]["name"], "Alice");
        assert_eq!(response_array[0]["age"], 25);
        assert_eq!(response_array[1]["name"], "Bob");
        assert_eq!(response_array[1]["age"], 30);

        // Cleanup
        source.close().await.unwrap();
        processor.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_backpressure() {

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let (mut processor, mut source) = create_test_processor_and_source(schema.clone()).await;
        let mut runtime_context = create_test_runtime_context();
        runtime_context.set_request_sink_source_response_sender(processor.get_response_sender());
        runtime_context.set_request_sink_source_request_receiver(processor.get_shared_request_receiver().clone());
        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        // Get the actual bound address after server starts
        let bind_address = processor.config.bind_address.clone();
        let max_pending = processor.config.max_pending_requests;
        
        // Wait a bit for server to start
        sleep(Duration::from_millis(100)).await;
        
        let client = reqwest::Client::new();
        let test_payload = serde_json::json!({
            "data": {
                "payload": [{"name": "Test", "age": 25}]
            }
        });
        
        // Send requests in parallel without awaiting - store futures
        let mut request_futures = Vec::new();
        let total_requests = max_pending + 3; // Send more than capacity to trigger backpressure
        
        for i in 0..total_requests {
            let client = client.clone();
            let bind_address = bind_address.clone();
            let test_payload = test_payload.clone();
            
            let future = tokio::spawn(async move {
                client
                    .post(&format!("http://{}/request", bind_address))
                    .json(&test_payload)
                    .send()
                    .await
            });
            
            request_futures.push((i, future));
        }
        
        // Wait a short time for requests to be processed by the server
        sleep(Duration::from_millis(100)).await;
        
        // Check the current state - some requests should be pending
        let pending_count = processor.get_pending_count();
        
        // We should have exactly max_pending requests in pending state
        assert_eq!(pending_count, max_pending, "Should have exactly max_pending requests pending");
        
        // Now check all futures - some should complete immediately with TOO_MANY_REQUESTS
        let mut still_pending = 0;
        let mut too_many_requests_count = 0;
        
        for (i, future) in request_futures {
            // Check if the future is ready
            match future.now_or_never() {
                Some(Ok(Ok(response))) => {
                    // Request completed immediately - must be TOO_MANY_REQUESTS due to backpressure
                    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS, 
                              "Request {} completed immediately but with unexpected status: {}", i, response.status());
                    too_many_requests_count += 1;
                }
                Some(Ok(Err(e))) => {
                    panic!("Request {} failed immediately with error: {}", i, e);
                }
                Some(Err(e)) => {
                    panic!("Request {} task failed: {}", i, e);
                }
                None => {
                    // Request is still pending
                    still_pending += 1;
                }
            }
        }
        
        assert_eq!(still_pending, max_pending, "Number of pending requests should equal max_pending capacity");
        assert_eq!(too_many_requests_count + still_pending, total_requests, "All requests should be accounted for");
        
        // Cleanup
        source.close().await.unwrap();
        processor.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_request_timeout() {
        // Create config with very short timeout
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let mut config = create_test_config(schema.clone());
        config.request_timeout_ms = 100; // 100ms timeout
        
        let mut processor = RequestSourceProcessor::new(config);
        processor.start().await.unwrap();
        
        let mut source = HttpRequestSourceFunction::new(schema.clone());
        let mut runtime_context = create_test_runtime_context();
        runtime_context.set_request_sink_source_response_sender(processor.get_response_sender());
        runtime_context.set_request_sink_source_request_receiver(processor.get_shared_request_receiver().clone());

        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        let bind_address = processor.config.bind_address.clone();
        
        // Wait a bit for server to start
        sleep(Duration::from_millis(50)).await;
        
        let client = reqwest::Client::new();
        let test_payload = serde_json::json!({
            "data": {
                "payload": [{"name": "Test", "age": 25}]
            }
        });
        
        // Send a request but don't process it (don't call fetch() and don't process responses)
        let response = client
            .post(&format!("http://{}/request", bind_address))
            .json(&test_payload)
            .send()
            .await
            .unwrap();
        
        // Should get REQUEST_TIMEOUT due to no response processing
        assert_eq!(response.status(), reqwest::StatusCode::REQUEST_TIMEOUT);
        assert_eq!(processor.get_pending_count(), 0);
        
        // Cleanup
        source.close().await.unwrap();
        processor.stop().await.unwrap();
    }

}

