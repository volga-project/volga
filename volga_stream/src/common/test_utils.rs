use std::sync::Arc;
use arrow::array::{StringArray, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Mutex;
use lazy_static::lazy_static;

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
