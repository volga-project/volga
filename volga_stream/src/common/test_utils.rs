use std::sync::Arc;
use arrow::array::StringArray;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use arrow::compute::concat_batches;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Mutex;
use lazy_static::lazy_static;
use async_trait::async_trait;

use crate::common::Message;
use crate::runtime::functions::map::MapFunctionTrait;
use crate::runtime::worker::WorkerState;

pub fn create_test_string_batch(data: Vec<String>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
    ]);
    
    let array = StringArray::from(data);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

lazy_static! {
    static ref USED_PORTS: Mutex<HashMap<u16, bool>> = Mutex::new(HashMap::new());
}

pub fn gen_unique_grpc_port() -> u16 {
    let mut used_ports = USED_PORTS.lock().unwrap();
    
    loop {
        let port = rand::thread_rng().gen_range(50000, 60000);
        
        // Check if port is already used
        if !used_ports.contains_key(&port) {
            used_ports.insert(port, true);
            return port;
        }
    }
}

pub fn print_worker_metrics(worker_state: &WorkerState) {
    // print metrics
    println!("\n=== Worker Metrics ===");
    println!("Task Statuses:");
    for (vertex_id, status) in &worker_state.task_statuses {
        println!("  {}: {:?}", vertex_id, status);
    }
    
    println!("\nTask Metrics:");
    for (vertex_id, metrics) in &worker_state.task_metrics {
        println!("  {}:", vertex_id);
        println!("    Messages: {}", metrics.num_messages);
        println!("    Records: {}", metrics.num_records);
        println!("    Latency Histogram: {:?}", metrics.latency_histogram);
    }
    
    println!("\nAggregated Metrics:");
    println!("  Total Messages: {}", worker_state.aggregated_metrics.total_messages);
    println!("  Total Records: {}", worker_state.aggregated_metrics.total_records);
    println!("  Latency Histogram: {:?}", worker_state.aggregated_metrics.latency_histogram);
    println!("===================\n");
}


#[derive(Debug, Clone)]
pub struct IdentityMapFunction;

#[async_trait]
impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> anyhow::Result<Message> {
        Ok(message)
    }
}

/// Verifies that all records from expected_messages match the records in actual_messages
/// by concatenating all record batches from each list and comparing the resulting batches.
/// This handles cases where batching may change the number of messages but preserves all records.
/// Watermark messages are filtered out before comparison.
pub fn verify_message_records_match(expected_messages: &[Message], actual_messages: &[Message], test_name: &str, preserve_order: bool) {
    // Filter out watermark messages from both lists
    let expected_data_messages: Vec<&Message> = expected_messages
        .iter()
        .filter(|msg| !matches!(msg, Message::Watermark(_)))
        .collect();
    
    let actual_data_messages: Vec<&Message> = actual_messages
        .iter()
        .filter(|msg| !matches!(msg, Message::Watermark(_)))
        .collect();

    if expected_data_messages.is_empty() && actual_data_messages.is_empty() {
        return; // Both empty, nothing to compare
    }
    
    assert!(!expected_data_messages.is_empty(), "{}: Expected data messages cannot be empty", test_name);
    assert!(!actual_data_messages.is_empty(), "{}: Actual data messages cannot be empty", test_name);

    // Get schema from the first data message (assuming all messages have the same schema)
    let expected_schema = expected_data_messages[0].record_batch().schema();
    let actual_schema = actual_data_messages[0].record_batch().schema();
    
    assert_eq!(expected_schema, actual_schema, 
        "{}: Schema mismatch between expected and actual messages", test_name);

    // Collect all record batches from expected data messages
    let expected_batches: Vec<RecordBatch> = expected_data_messages
        .iter()
        .map(|msg| msg.record_batch().clone())
        .collect();

    // Collect all record batches from actual data messages
    let actual_batches: Vec<RecordBatch> = actual_data_messages
        .iter()
        .map(|msg| msg.record_batch().clone())
        .collect();

    // Concatenate all expected batches into a single batch
    let expected_concat = concat_batches(&expected_schema, &expected_batches)
        .expect(&format!("{}: Failed to concatenate expected batches", test_name));

    // Concatenate all actual batches into a single batch
    let actual_concat = concat_batches(&actual_schema, &actual_batches)
        .expect(&format!("{}: Failed to concatenate actual batches", test_name));

    // Compare the concatenated batches
    assert_eq!(expected_concat.num_rows(), actual_concat.num_rows(),
        "{}: Expected {} total rows, got {} total rows", 
        test_name, expected_concat.num_rows(), actual_concat.num_rows());

    assert_eq!(expected_concat.num_columns(), actual_concat.num_columns(),
        "{}: Expected {} columns, got {} columns", 
        test_name, expected_concat.num_columns(), actual_concat.num_columns());

    // Sort batches if order doesn't need to be preserved
    let (expected_final, actual_final) = if preserve_order {
        (expected_concat, actual_concat)
    } else {
        use arrow::compute::kernels::sort::{sort_to_indices, SortOptions};
        
        let expected_indices = sort_to_indices(expected_concat.column(0), Some(SortOptions::default()), None).unwrap();
        let actual_indices = sort_to_indices(actual_concat.column(0), Some(SortOptions::default()), None).unwrap();
        
        // Create sorted batches
        let expected_sorted_columns: Vec<_> = (0..expected_concat.num_columns())
            .map(|col_idx| arrow::compute::take(expected_concat.column(col_idx), &expected_indices, None).unwrap())
            .collect();
        let actual_sorted_columns: Vec<_> = (0..actual_concat.num_columns())
            .map(|col_idx| arrow::compute::take(actual_concat.column(col_idx), &actual_indices, None).unwrap())
            .collect();
            
        let expected_sorted = RecordBatch::try_new(expected_concat.schema(), expected_sorted_columns).unwrap();
        let actual_sorted = RecordBatch::try_new(actual_concat.schema(), actual_sorted_columns).unwrap();
        
        (expected_sorted, actual_sorted)
    };

    // Compare columns
    for col_idx in 0..expected_final.num_columns() {
        let expected_column = expected_final.column(col_idx);
        let actual_column = actual_final.column(col_idx);
        
        assert_eq!(expected_column.data_type(), actual_column.data_type(),
            "{}: Column {} data type mismatch", test_name, col_idx);
        
        assert_eq!(expected_column.len(), actual_column.len(),
            "{}: Column {} length mismatch", test_name, col_idx);

        assert_eq!(expected_column.to_data(), actual_column.to_data(),
            "{}: Column {} data mismatch{}", test_name, col_idx, 
            if preserve_order { "" } else { " after sorting" });
    }
}