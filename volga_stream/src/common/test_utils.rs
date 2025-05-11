use std::sync::Arc;
use arrow::array::{StringArray, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use anyhow::Result;

pub fn create_test_string_batch(data: Vec<String>) -> Result<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
    ]);
    
    let array = StringArray::from(data);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])
        .map_err(|e| anyhow::anyhow!("Failed to create record batch: {}", e))
}
