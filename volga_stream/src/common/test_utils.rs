use std::sync::Arc;
use arrow::array::{StringArray, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Mutex;
use lazy_static::lazy_static;

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
