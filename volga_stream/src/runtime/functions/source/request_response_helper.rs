use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};

use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use super::request_source::{RequestSourceConfig, HttpRequestSourceFunction, SOURCE_VERTEX_ID_FIELD, SOURCE_REQUEST_ID_FIELD};

/// Helper function to create a connected request source and sink pair
/// Returns (source_config, sink_config) that are connected via channels
pub fn create_request_response_pair(
    bind_address: String,
    max_pending_requests: usize,
    request_timeout_ms: u64,
    data_schema: Arc<Schema>,
) -> Result<(SourceConfig, SinkConfig), String> {
    
    // Create the request source config with just the user data schema
    // Metadata fields are now stored in message extras, not in the schema
    let request_source_config = RequestSourceConfig::new(
        bind_address,
        max_pending_requests,
        request_timeout_ms,
        data_schema,
    );
    
    // Create HTTP request source function to get the response sender
    let http_source = HttpRequestSourceFunction::new(request_source_config.clone());
    let response_sender = http_source.get_response_sender()
        .expect("Failed to get response sender from HTTP source");
    
    let source_config = SourceConfig::HttpRequestSourceConfig(request_source_config);
    let sink_config = SinkConfig::RequestSinkConfig(response_sender);
    
    Ok((source_config, sink_config))
}
