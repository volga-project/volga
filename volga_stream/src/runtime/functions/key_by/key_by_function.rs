use std::sync::Arc;
use async_trait::async_trait;
use crate::common::message::{Message, KeyedMessage, BaseMessage};
use crate::common::Key;
use anyhow::Result;
use std::fmt;
use arrow::compute;
use arrow::compute::kernels::partition::partition;
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use std::collections::HashMap;
use std::collections::HashSet;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

pub trait KeyByFunctionTrait: Send + Sync + fmt::Debug {
    fn key_by(&self, batch: Message) -> Vec<KeyedMessage>;
}

/// Generic key-by function that can be used for any key-by function
#[derive(Debug, Clone)]
pub struct CustomKeyByFunction {
    function: Arc<dyn KeyByFunctionTrait>,
}

impl CustomKeyByFunction {
    pub fn new<F>(function: F) -> Self 
    where
        F: KeyByFunctionTrait + 'static,
    {
        Self {
            function: Arc::new(function),
        }
    }
}

impl KeyByFunctionTrait for CustomKeyByFunction {
    fn key_by(&self, batch: Message) -> Vec<KeyedMessage> {
        self.function.key_by(batch)
    }
}

/// Arrow-based key-by function using RecordBatch for stable key representation
#[derive(Debug, Clone)]
pub struct ArrowKeyByFunction {
    key_columns: Vec<String>,
}

impl ArrowKeyByFunction {
    pub fn new(key_columns: Vec<String>) -> Self {
        Self { key_columns }
    }

    /// Fast lexicographic sort implementation using arrow-row
    fn fast_lexsort_indices(&self, arrays: &[ArrayRef]) -> UInt32Array {
        // Create SortField objects for each array
        let fields: Vec<SortField> = arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect();
        
        // Create a RowConverter for the fields
        let converter = RowConverter::new(fields).unwrap();
        
        // Convert columns to row format
        let rows = converter.convert_columns(arrays).unwrap();
        
        // Sort the rows
        let mut indices: Vec<_> = rows.iter().enumerate().collect();
        indices.sort_by(|(_, a), (_, b)| a.cmp(b));
        
        // Create the UInt32Array of sorted indices
        UInt32Array::from_iter_values(indices.iter().map(|(i, _)| *i as u32))
    }
}

impl KeyByFunctionTrait for ArrowKeyByFunction {
    fn key_by(&self, message: Message) -> Vec<KeyedMessage> {
        let record_batch = message.record_batch();
        let schema = record_batch.schema();
        
        // If batch is empty, return empty result
        if record_batch.num_rows() == 0 {
            return Vec::new();
        }
        
        // Step 1: Identify the key columns
        let mut key_column_indices = Vec::with_capacity(self.key_columns.len());
        let mut key_fields = Vec::with_capacity(self.key_columns.len());
        let mut key_arrays = Vec::with_capacity(self.key_columns.len());
        
        for key_column in &self.key_columns {
            if let Some((idx, field)) = schema.column_with_name(key_column) {
                key_column_indices.push(idx);
                key_fields.push(field.clone());
                key_arrays.push(record_batch.column(idx).clone());
            } else {
                panic!("Key column '{}' not found", key_column);
            }
        }
        
        // Step 2: Sort the data using arrow-row's fast sorting
        let sorted_indices = self.fast_lexsort_indices(&key_arrays);
        
        // Step 3: Create sorted arrays for all columns
        let mut sorted_columns = Vec::with_capacity(record_batch.num_columns());
        for i in 0..record_batch.num_columns() {
            let sorted_array = compute::take(record_batch.column(i).as_ref(), &sorted_indices, None).unwrap();
            sorted_columns.push(sorted_array);
        }
        
        // Create a new sorted batch
        let sorted_batch = RecordBatch::try_new(record_batch.schema(), sorted_columns).unwrap();
        
        // Step 4: Extract sorted key columns
        let sorted_key_arrays: Vec<ArrayRef> = key_column_indices
            .iter()
            .map(|&idx| sorted_batch.column(idx).clone())
            .collect();
        
        // Step 5: Use Arrow's partition function to find ranges with equal keys
        let partitions = partition(&sorted_key_arrays).unwrap();
        let ranges = partitions.ranges();
        
        // Step 6: Create keyed batches for each partition
        let mut keyed_batches = Vec::with_capacity(ranges.len());
        
        for range in ranges {
            // Extract this range from the sorted batch
            let group_batch = sorted_batch.slice(range.start, range.end - range.start);
            
            // Create a key from the first row of the key columns
            let key_arrays_for_first_row: Vec<ArrayRef> = key_column_indices
                .iter()
                .map(|&idx| {
                    let array = group_batch.column(idx);
                    let indices = UInt32Array::from(vec![0]);  // Get just the first row
                    compute::take(array.as_ref(), &indices, None).unwrap()
                })
                .collect();
            
            // Create key RecordBatch with a single row
            let key_schema = Arc::new(Schema::new(key_fields.clone()));
            let key_batch = RecordBatch::try_new(key_schema, key_arrays_for_first_row).unwrap();
            
            // Create Key object
            let key = Key::new(key_batch).unwrap();
            
            // Create a KeyedDataBatch with this partition data
            let keyed_batch = KeyedMessage::new(
                BaseMessage::new(message.upstream_vertex_id(), group_batch),
                key,
            );
            
            keyed_batches.push(keyed_batch);
        }
        
        keyed_batches
    }
}

#[derive(Debug, Clone)]
pub enum KeyByFunction {
    Custom(CustomKeyByFunction),
    Arrow(ArrowKeyByFunction),
}

impl KeyByFunctionTrait for KeyByFunction {
    fn key_by(&self, message: Message) -> Vec<KeyedMessage> {
        match self {
            KeyByFunction::Custom(function) => function.key_by(message),
            KeyByFunction::Arrow(function) => function.key_by(message),
        }
    }
}

impl KeyByFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: KeyByFunctionTrait + 'static,
    {
        Self::Custom(CustomKeyByFunction::new(function))
    }
    
    pub fn new_arrow_key_by(key_columns: Vec<String>) -> Self {
        Self::Arrow(ArrowKeyByFunction::new(key_columns))
    }
}

#[async_trait]
impl FunctionTrait for KeyByFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Float64Array, StringArray};
    use arrow::datatypes::{Field, DataType};

    #[tokio::test]
    async fn test_arrow_key_by_single_column() {
        // Create a test batch with a single key column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 1]);
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(value_array)]
        ).unwrap();
        
        let message = Message::new(None, record_batch);
        
        // Create a key-by function with 'id' as the key column
        let key_by_function = ArrowKeyByFunction::new(vec!["id".to_string()]);
        
        // Execute key-by
        let keyed_messages = key_by_function.key_by(message);
        
        // We should have 3 distinct keys (1, 2, 3)
        assert_eq!(keyed_messages.len(), 3);
        
        // Verify the contents of each keyed message
        let mut id_to_values: HashMap<i32, Vec<String>> = HashMap::new();
        
        for keyed_message in &keyed_messages {
            let batch = &keyed_message.base.record_batch;
            let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let value_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            
            // All rows in a batch should have the same id
            let id = id_col.value(0);
            assert!(id_col.iter().all(|val| val == Some(id)), 
                "Not all IDs in batch are equal: {:?}", id_col);
            
            // Collect values for this id
            let values: Vec<String> = (0..batch.num_rows())
                .map(|i| value_col.value(i).to_string())
                .collect();
            
            id_to_values.insert(id, values);
        }
        
        // Verify exact contents of each group
        assert_eq!(id_to_values.len(), 3);
        
        // ID 1 should have values "a", "c", "f" in exact order
        let values_for_id_1 = id_to_values.get(&1).unwrap();
        assert_eq!(values_for_id_1.len(), 3);
        assert_eq!(*values_for_id_1, vec!["a".to_string(), "c".to_string(), "f".to_string()]);
        
        // ID 2 should have values "b", "e" in exact order
        let values_for_id_2 = id_to_values.get(&2).unwrap();
        assert_eq!(values_for_id_2.len(), 2);
        assert_eq!(*values_for_id_2, vec!["b".to_string(), "e".to_string()]);
        
        // ID 3 should have only value "d"
        let values_for_id_3 = id_to_values.get(&3).unwrap();
        assert_eq!(values_for_id_3.len(), 1);
        assert_eq!(values_for_id_3[0], "d");
    }

    #[tokio::test]
    async fn test_arrow_key_by_multiple_columns() {
        // Create a test batch with multiple key columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("subcategory", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let category_array = Int32Array::from(vec![1, 1, 1, 2, 2, 3]);
        let subcategory_array = Int32Array::from(vec![1, 2, 1, 1, 1, 1]);
        let value_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(category_array), 
                Arc::new(subcategory_array), 
                Arc::new(value_array)
            ]
        ).unwrap();
        
        let message = Message::new(None, record_batch);
        
        // Create a key-by function with both columns as keys
        let key_by_function = ArrowKeyByFunction::new(
            vec!["category".to_string(), "subcategory".to_string()]
        );
        
        // Execute key-by
        let keyed_messages = key_by_function.key_by(message);
        
        // We should have 4 distinct keys: (1,1), (1,2), (2,1), (3,1)
        assert_eq!(keyed_messages.len(), 4);
        
        // Map to store key -> values
        let mut key_to_values: HashMap<(i32, i32), Vec<f64>> = HashMap::new();
        
        for keyed_message in &keyed_messages {
            let batch = &keyed_message.base.record_batch;
            let cat_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let subcat_col = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            let value_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            
            // All rows should have the same category and subcategory
            let cat = cat_col.value(0);
            let subcat = subcat_col.value(0);
            
            // Collect all values for this key
            let values: Vec<f64> = (0..batch.num_rows())
                .map(|i| value_col.value(i))
                .collect();
            
            key_to_values.insert((cat, subcat), values);
        }
        
        // Verify specific contents for each key group
        assert_eq!(key_to_values.len(), 4);
        
        // Key (1,1) should have values [10.0, 30.0] in exact order
        let values_1_1 = key_to_values.get(&(1, 1)).unwrap();
        assert_eq!(values_1_1.len(), 2);
        assert_eq!(*values_1_1, vec![10.0, 30.0]);
        
        // Key (1,2) should have only value 20.0
        let values_1_2 = key_to_values.get(&(1, 2)).unwrap();
        assert_eq!(values_1_2.len(), 1);
        assert_eq!(*values_1_2, vec![20.0]);
        
        // Key (2,1) should have values [40.0, 50.0] in exact order
        let values_2_1 = key_to_values.get(&(2, 1)).unwrap();
        assert_eq!(values_2_1.len(), 2);
        assert_eq!(*values_2_1, vec![40.0, 50.0]);
        
        // Key (3,1) should have only value 60.0
        let values_3_1 = key_to_values.get(&(3, 1)).unwrap();
        assert_eq!(values_3_1.len(), 1);
        assert_eq!(*values_3_1, vec![60.0]);
    }
} 