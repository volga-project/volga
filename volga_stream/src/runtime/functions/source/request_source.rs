use async_trait::async_trait;
use anyhow::Result;
use std::sync::Arc;
use std::any::Any;
use tokio::sync::{mpsc, oneshot};
use dashmap::DashMap;
use tokio::time::{timeout, Duration};
use axum::{
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use arrow::datatypes::Schema;
use super::json_utils::{record_batch_to_json, json_to_record_batch};
use std::collections::HashMap;
use uuid::Uuid;

use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use super::source_function::SourceFunctionTrait;

/// Reserved metadata field names
pub const SOURCE_VERTEX_ID_FIELD: &str = "_source_vertex_id";
pub const SOURCE_REQUEST_ID_FIELD: &str = "_source_request_id";

/// Configuration for request source
#[derive(Debug, Clone)]
pub struct RequestSourceConfig {
    pub bind_address: String,
    pub max_pending_requests: usize,
    pub request_timeout_ms: u64,
    pub schema: Arc<Schema>,
}

impl RequestSourceConfig {
    pub fn new(bind_address: String, max_pending_requests: usize, request_timeout_ms: u64, schema: Arc<Schema>) -> Self {
        Self {
            bind_address,
            max_pending_requests,
            request_timeout_ms,
            schema,
        }
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
struct PendingRequest {
    request_id: String,
    payload: RequestPayload,
}

/// HTTP-based request source function
#[derive(Debug)]
pub struct HttpRequestSourceFunction {
    config: RequestSourceConfig,
    runtime_context: Option<RuntimeContext>,
    
    // Channel for receiving incoming requests from HTTP server
    request_rx: Option<mpsc::Receiver<PendingRequest>>,
    
    // Channel for receiving processed responses from sink
    response_rx: Option<mpsc::Receiver<Message>>,
    response_tx: Option<mpsc::Sender<Message>>,
    
    // Map to track pending requests by request_id
    pending_requests: Arc<DashMap<String, oneshot::Sender<ResponsePayload>>>,
    
    // Server handle for shutdown
    server_handle: Option<tokio::task::JoinHandle<()>>,
    
    // Response processing task handle
    response_processor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HttpRequestSourceFunction {
    pub fn new(config: RequestSourceConfig) -> Self {
        let (response_tx, response_rx) = mpsc::channel(1000);
        
        Self {
            config,
            runtime_context: None,
            request_rx: None, // Will be set when server starts
            response_rx: Some(response_rx),
            response_tx: Some(response_tx),
            pending_requests: Arc::new(DashMap::new()),
            server_handle: None,
            response_processor_handle: None,
        }
    }
    
    pub fn get_response_sender(&self) -> Option<mpsc::Sender<Message>> {
        self.response_tx.clone()
    }
    
    pub fn get_pending_count(&self) -> usize {
         self.pending_requests.len()
    }
    
    fn create_message_from_request(&self, request_id: &str, payload: &RequestPayload) -> Result<Message> {
        let vertex_id = self.runtime_context.as_ref().expect("Runtime context not set").vertex_id().to_string();
        
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
        
        let record_batch = json_to_record_batch(&payload_array)?;
        
        let mut extras = HashMap::new();
        extras.insert(SOURCE_VERTEX_ID_FIELD.to_string(), vertex_id);
        extras.insert(SOURCE_REQUEST_ID_FIELD.to_string(), request_id.to_string());
        
        let message = Message::new(
            None, // upstream_vertex_id - will be set by the runtime
            record_batch,
            None, // ingest_timestamp - will be set by the runtime
            Some(extras)
        );
        
        Ok(message)
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
    
    async fn response_processor_task(
        mut response_rx: mpsc::Receiver<Message>,
        pending_requests: Arc<DashMap<String, oneshot::Sender<ResponsePayload>>>,
    ) {
        while let Some(response) = response_rx.recv().await {
            if let Err(e) = Self::process_single_response(response, &pending_requests).await {
                eprintln!("Error processing response: {}", e);
            }
        }
    }
    
    async fn process_single_response(
        response: Message,
        pending_requests: &Arc<DashMap<String, oneshot::Sender<ResponsePayload>>>,
    ) -> Result<()> {
        let request_id = response.get_extras().and_then(|extras| extras.get(SOURCE_REQUEST_ID_FIELD).cloned())
            .ok_or_else(|| anyhow::anyhow!("Response missing {} in extras", SOURCE_REQUEST_ID_FIELD))?;
        
        let response_tx = pending_requests.remove(&request_id).map(|(_, tx)| tx);
        
        if let Some(response_tx) = response_tx {
            let record_batch = response.record_batch();
            let response_data = match record_batch_to_json(record_batch) {
                Ok(json_array) => json_array,
                Err(e) => {
                    eprintln!("Error converting RecordBatch to JSON: {}", e);
                    serde_json::json!({"error": "Failed to convert response to JSON"})
                }
            };
            
            let response_payload = ResponsePayload {
                data: response_data,
            };
            
            // Send response back to HTTP client (ignore if client disconnected)
            let _ = response_tx.send(response_payload);
        }   
        
        Ok(())
    }
    
    fn start_server(&mut self, _runtime_context: &RuntimeContext) -> Result<()> {
        let (request_tx, request_rx) = mpsc::channel(self.config.max_pending_requests);
        
        self.request_rx = Some(request_rx);
        
        let bind_address = self.config.bind_address.clone();
        let max_pending = self.config.max_pending_requests;
        let request_timeout_ms = self.config.request_timeout_ms;
        let pending_requests = self.pending_requests.clone();
        
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
    
    fn stop_server(&mut self) -> Result<()> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn stop_response_processor(&mut self) -> Result<()> {
        if let Some(handle) = self.response_processor_handle.take() {
            handle.abort();
        }
        Ok(())
    }
}

async fn handle_request(
    request_tx: mpsc::Sender<PendingRequest>,
    max_pending_requests: usize,
    request_timeout_ms: u64,
    pending_requests: Arc<DashMap<String, oneshot::Sender<ResponsePayload>>>,
    Json(payload): Json<RequestPayload>,
) -> Result<Json<ResponsePayload>, StatusCode> {
    if pending_requests.len() >= max_pending_requests {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    
    let request_id = Uuid::new_v4().to_string();
    let (response_tx, response_rx) = oneshot::channel();
    
    pending_requests.insert(request_id.clone(), response_tx);
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
        self.start_server(context)?;
        self.start_response_processor()?;
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        self.stop_server()?;
        self.stop_response_processor()?;
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
        let request_rx = self.request_rx.as_mut().expect("Request receiver not set");
        // TODO can we batch requests if channel has multiple?
        if let Ok(pending_request) = request_rx.try_recv() {
            match self.create_message_from_request(&pending_request.request_id, &pending_request.payload) {
                Ok(message) => {
                    return Some(message);
                }
                Err(e) => {
                    eprintln!("Error creating record batch: {}", e);
                    // Send error response and remove from pending
                    let error_response = ResponsePayload {
                        data: serde_json::json!({"error": e.to_string()}),
                    };
                    
                    if let Some((_, tx)) = self.pending_requests.remove(&pending_request.request_id) {
                        let _ = tx.send(error_response);
                    }
                }
            }
        }
        
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::runtime_context::RuntimeContext;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};
    use futures::FutureExt;
    use rand;

    fn create_test_config() -> RequestSourceConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        
        // Use a random high port to avoid conflicts
        let port = 8000 + (rand::random::<u16>() % 1000);
        
        RequestSourceConfig::new(
            format!("127.0.0.1:{}", port),
            2, // max_pending_requests
            1000, // request_timeout_ms
            schema,
        )
    }

    fn create_test_runtime_context() -> RuntimeContext {
        RuntimeContext::new("test_vertex".to_string(), 0, 1, None)
    }

    #[tokio::test]
    async fn test_fetch_and_request_response_cycle() {
        let mut source = HttpRequestSourceFunction::new(create_test_config());
        let runtime_context = create_test_runtime_context();
        
        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        let bind_address = source.config.bind_address.clone();
        
        // Wait a bit for server to start
        sleep(Duration::from_millis(50)).await;
        
        // Verify initial state
        assert_eq!(source.get_pending_count(), 0);
        
        let client = reqwest::Client::new();
        let test_payload = serde_json::json!({
            "data": {
                "payload": [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30}
                ]
            }
        });
        
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
        assert_eq!(source.get_pending_count(), 1);
        
        // Call fetch() to get the message from the HTTP request
        let message = source.fetch().await.expect("Should receive a message from HTTP request");
        
        // Verify the message structure and content
        let record_batch = message.record_batch();
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);
        
        // Verify extras contain correct metadata
        let extras = message.get_extras().unwrap();
        assert_eq!(extras.get(SOURCE_VERTEX_ID_FIELD).unwrap(), "test_vertex");
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
        assert_eq!(source.pending_requests.len(), 1);
        assert!(source.pending_requests.contains_key(request_id));
        
        // Simulate processing by creating a response message with the same extras
        let response_message = Message::new(
            Some("processor".to_string()),
            record_batch.clone(),
            Some(12345),
            Some(extras.clone())
        );
        
        // Send response back through the response channel
        if let Some(response_tx) = source.get_response_sender() {
            response_tx.send(response_message).await.unwrap();
        }
        
        // Wait a bit for response processing
        sleep(Duration::from_millis(50)).await;
        
        // Verify internal state: pending request should be removed after response processing
        assert!(!source.pending_requests.contains_key(request_id));
        assert_eq!(source.get_pending_count(), 0);
        
        // Verify the HTTP request completes successfully
        let response = request_handle.await.unwrap().unwrap();
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

        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_backpressure() {
        let mut source = HttpRequestSourceFunction::new(create_test_config());
        let runtime_context = create_test_runtime_context();
        
        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        // Get the actual bound address after server starts
        let bind_address = source.config.bind_address.clone();
        let max_pending = source.config.max_pending_requests;
        
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
        let pending_count = source.get_pending_count();
        println!("Pending requests: {}, Max allowed: {}", pending_count, max_pending);
        
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
                    println!("Request {} got TOO_MANY_REQUESTS immediately", i);
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
                    println!("Request {} is still pending", i);
                }
            }
        }
        
        assert_eq!(still_pending, max_pending, "Number of pending requests should equal max_pending capacity");
        assert_eq!(too_many_requests_count + still_pending, total_requests, "All requests should be accounted for");
        
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_request_timeout() {
        // Create config with very short timeout
        let mut config = create_test_config();
        config.request_timeout_ms = 100; // 100ms timeout
        
        let mut source = HttpRequestSourceFunction::new(config);
        let runtime_context = create_test_runtime_context();
        
        // Open the source function
        source.open(&runtime_context).await.unwrap();
        
        let bind_address = source.config.bind_address.clone();
        
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
        assert_eq!(source.get_pending_count(), 0);
        
        source.close().await.unwrap();
    }
}

