use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json::writer::LineDelimitedWriter;
use arrow_json::{ReaderBuilder};
use arrow_json::reader::infer_json_schema;
use serde_json::Value;
use std::io::Cursor;
use std::sync::Arc;

pub fn record_batch_to_json(record_batch: &RecordBatch) -> Result<Value> {
    let mut buffer = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write_batches(&[record_batch])?;
        writer.finish()?;
    }
    
    let json_string = String::from_utf8(buffer)
        .map_err(|e| anyhow::anyhow!("Failed to convert buffer to UTF-8: {}", e))?;
    
    // Parse each line and collect into an array
    let json_objects: Vec<Value> = json_string
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
    
    Ok(Value::Array(json_objects))
}

pub fn json_to_record_batch(json_value: &Value, schema: SchemaRef) -> Result<RecordBatch> {
    // Ensure we have an array
    let json_array = match json_value {
        Value::Array(arr) => arr,
        _ => return Err(anyhow::anyhow!("Expected JSON array, got: {:?}", json_value)),
    };
    
    // Convert to line-delimited JSON for schema inference and parsing
    // This is required because Arrow expects individual JSON objects, not JSON arrays
    let json_string = json_array.iter()
        .map(|obj| serde_json::to_string(obj))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to serialize JSON object: {}", e))?
        .join("\n");
    
    let json_bytes = json_string.into_bytes();
    
    // Use provided schema or infer it
    // let schema = if let Some(schema) = schema {
    //     schema
    // } else {
    //     let mut cursor_for_schema = Cursor::new(&json_bytes);
    //     let (inferred_schema, _) = infer_json_schema(&mut cursor_for_schema, None)
    //         .map_err(|e| anyhow::anyhow!("Failed to infer JSON schema: {}", e))?;
    //     Arc::new(inferred_schema)
    // };
    
    let cursor = Cursor::new(json_bytes);
    let mut reader = ReaderBuilder::new(schema).build(cursor)?;
    
    // Read the first and only batch
    reader.next()
        .ok_or_else(|| anyhow::anyhow!("No data found in JSON"))?
        .map_err(|e| anyhow::anyhow!("Failed to read RecordBatch: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    
    #[test]
    fn test_record_batch_to_json() {
        // Create a simple RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        
        let name_array = StringArray::from(vec!["Alice", "Bob"]);
        let age_array = Int64Array::from(vec![25, 30]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(name_array), Arc::new(age_array)],
        ).unwrap();
        
        // Convert to JSON
        let json_result = record_batch_to_json(&record_batch).unwrap();
        
        // Verify it's an array
        assert!(json_result.is_array());
        let json_array = json_result.as_array().unwrap();
        assert_eq!(json_array.len(), 2);
        
        // Check first object
        let first_obj = &json_array[0];
        assert_eq!(first_obj["name"], "Alice");
        assert_eq!(first_obj["age"], 25);
        
        // Check second object
        let second_obj = &json_array[1];
        assert_eq!(second_obj["name"], "Bob");
        assert_eq!(second_obj["age"], 30);
    }
    
    #[test]
    fn test_json_to_record_batch() {
        // Create JSON array
        let json_value = serde_json::json!([
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30}
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        
        // Convert to RecordBatch
        let record_batch = json_to_record_batch(&json_value, schema).unwrap();
        
        // Verify the RecordBatch
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);
        
        // Check schema
        let schema = record_batch.schema();
        assert_eq!(schema.fields().len(), 2);
        
        // Find the name column by field name (Arrow may reorder columns)
        let name_column_index = schema.column_with_name("name").unwrap().0;
        let name_column = record_batch.column(name_column_index);
        let name_array = name_column.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        
        // Find the age column by field name
        let age_column_index = schema.column_with_name("age").unwrap().0;
        let age_column = record_batch.column(age_column_index);
        // Age might be inferred as Int64 by Arrow
        if let Some(age_array) = age_column.as_any().downcast_ref::<arrow::array::Int64Array>() {
            assert_eq!(age_array.value(0), 25);
            assert_eq!(age_array.value(1), 30);
        } else {
            panic!("Age column should be Int64Array, got: {:?}", age_column.data_type());
        }
    }
}
