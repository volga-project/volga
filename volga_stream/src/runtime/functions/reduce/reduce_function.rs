use std::sync::Arc;
use async_trait::async_trait;
use crate::common::message::{Message, KeyedMessage, BaseMessage};
use crate::common::Key;
use anyhow::Result;
use std::fmt;
use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute;
use arrow::compute::kernels::aggregate::{sum, min, max};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

#[derive(Debug, Clone)]
pub struct Accumulator {
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: i64,
}

impl Accumulator {
    pub fn new(initial_min: f64, initial_max: f64, initial_sum: f64, initial_count: i64) -> Self {
        Self { 
            min: initial_min,
            max: initial_max,
            sum: initial_sum,
            count: initial_count,
        }
    }
    
    pub fn update(&mut self, min: f64, max: f64, sum: f64, count: i64) {
        if count > 0 {
            self.min = self.min.min(min);
            self.max = self.max.max(max);
            self.sum += sum;
            self.count += count;
        }
    }
    
    pub fn average(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregationResult {
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: f64,
    pub average: f64,
}

pub trait ReduceFunctionTrait: Send + Sync + fmt::Debug {
    fn create_accumulator(&self) -> Accumulator;
    
    fn update_accumulator(&self, accumulator: &mut Accumulator, message: &KeyedMessage);
    
    fn get_result(&self, accumulator: &Accumulator) -> AggregationResult;
}

/// Trait for extracting final results from aggregated data
pub trait AggregationResultExtractorTrait: Send + Sync + fmt::Debug {
    fn extract_result(&self, key: &Key, result: &AggregationResult, upstream_vertex_id: Option<String>, ingest_timestamp: Option<u64>) -> Message;
}

/// Default implementation of ResultExtractor that includes all aggregation values
#[derive(Debug, Clone)]
pub struct AllAggregationsResultExtractor;

impl AggregationResultExtractorTrait for AllAggregationsResultExtractor {
    fn extract_result(&self, key: &Key, result: &AggregationResult, upstream_vertex_id: Option<String>, ingest_timestamp: Option<u64>) -> Message {
        // Create a schema with all aggregation fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("min", DataType::Float64, false),
            Field::new("max", DataType::Float64, false),
            Field::new("sum", DataType::Float64, false),
            Field::new("count", DataType::Float64, false),
            Field::new("average", DataType::Float64, false),
        ]));
        
        let min_array = Float64Array::from(vec![result.min]);
        let max_array = Float64Array::from(vec![result.max]);
        let sum_array = Float64Array::from(vec![result.sum]);
        let count_array = Float64Array::from(vec![result.count]);
        let avg_array = Float64Array::from(vec![result.average]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(min_array),
                Arc::new(max_array), 
                Arc::new(sum_array),
                Arc::new(count_array),
                Arc::new(avg_array),
            ]
        ).unwrap();
        
        Message::Keyed(KeyedMessage::new(
            BaseMessage::new(upstream_vertex_id, record_batch, ingest_timestamp),
            key.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum AggregationType {
    Min,
    Max,
    Sum,
    Count,
    Average,
}

#[derive(Debug, Clone)]
pub struct SingleAggregationResultExtractor {
    aggregation_type: AggregationType,
    field_name: String,
}

impl SingleAggregationResultExtractor {
    pub fn new(aggregation_type: AggregationType, field_name: String) -> Self {
        Self {
            aggregation_type,
            field_name,
        }
    }
}

impl AggregationResultExtractorTrait for SingleAggregationResultExtractor {
    fn extract_result(&self, key: &Key, result: &AggregationResult, upstream_vertex_id: Option<String>, ingest_timestamp: Option<u64>) -> Message {
        let value = match self.aggregation_type {
            AggregationType::Min => result.min,
            AggregationType::Max => result.max,
            AggregationType::Sum => result.sum,
            AggregationType::Count => result.count,
            AggregationType::Average => result.average,
        };
        
        let schema = Arc::new(Schema::new(vec![
            Field::new(&self.field_name, DataType::Float64, false),
        ]));
        
        let array = Float64Array::from(vec![value]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(array)]
        ).unwrap();
        
        Message::Keyed(KeyedMessage::new(
            BaseMessage::new(upstream_vertex_id, record_batch, ingest_timestamp),
            key.clone(),
        ))
    }
}

/// Wrapper enum for different result extractors
#[derive(Debug, Clone)]
pub enum AggregationResultExtractor {
    All(AllAggregationsResultExtractor),
    Single(SingleAggregationResultExtractor),
    Custom(Arc<dyn AggregationResultExtractorTrait>),
}

impl AggregationResultExtractorTrait for AggregationResultExtractor {
    fn extract_result(&self, key: &Key, result: &AggregationResult, upstream_vertex_id: Option<String>, ingest_timestamp: Option<u64>) -> Message {
        match self {
            AggregationResultExtractor::All(e) => e.extract_result(key, result, upstream_vertex_id, ingest_timestamp),
            AggregationResultExtractor::Single(e) => e.extract_result(key, result, upstream_vertex_id, ingest_timestamp),
            AggregationResultExtractor::Custom(e) => e.extract_result(key, result, upstream_vertex_id, ingest_timestamp),
        }
    }
}

impl AggregationResultExtractor {
    pub fn all_aggregations() -> Self {
        Self::All(AllAggregationsResultExtractor {})
    }
    
    pub fn single_aggregation(aggregation_type: AggregationType, field_name: String) -> Self {
        Self::Single(SingleAggregationResultExtractor::new(aggregation_type, field_name))
    }
    
    pub fn custom<E: AggregationResultExtractorTrait + 'static>(extractor: E) -> Self {
        Self::Custom(Arc::new(extractor))
    }
}

/// Generic reduce function that can be customized with user logic
#[derive(Debug, Clone)]
pub struct CustomReduceFunction {
    function: Arc<dyn ReduceFunctionTrait>,
}

impl CustomReduceFunction {
    pub fn new<F>(function: F) -> Self 
    where
        F: ReduceFunctionTrait + 'static,
    {
        Self {
            function: Arc::new(function),
        }
    }
}

impl ReduceFunctionTrait for CustomReduceFunction {
    fn create_accumulator(&self) -> Accumulator {
        self.function.create_accumulator()
    }
    
    fn update_accumulator(&self, accumulator: &mut Accumulator, message: &KeyedMessage) {
        self.function.update_accumulator(accumulator, message)
    }
    
    fn get_result(&self, accumulator: &Accumulator) -> AggregationResult {
        self.function.get_result(accumulator)
    }
}

/// Arrow-based implementation for common aggregation functions
#[derive(Debug, Clone)]
pub struct ArrowReduceFunction {
    column_name: String,
}

impl ArrowReduceFunction {
    pub fn new(column_name: String) -> Self {
        Self { column_name }
    }
    
    fn compute_aggregations(&self, array: &ArrayRef) -> (f64, f64, f64, i64) {
        let count = array.len() as i64;  // Always return the array length for count
        
        // Try to convert to float array and compute aggregations
        match compute::cast(array, &DataType::Float64) {
            Ok(array_f64) => {
                if let Some(float_array) = array_f64.as_any().downcast_ref::<Float64Array>() {
                    // Compute aggregations for valid float array
                    let min_val = min(float_array).unwrap_or(f64::INFINITY);
                    let max_val = max(float_array).unwrap_or(f64::NEG_INFINITY);
                    let sum_val = sum(float_array).unwrap_or(0.0);
                    (min_val, max_val, sum_val, count)
                } else {
                    // If downcast fails, return default values with actual count
                    (f64::INFINITY, f64::NEG_INFINITY, 0.0, count)
                }
            },
            Err(_) => {
                // If cast fails (e.g., for string arrays), return default values with actual count
                (f64::INFINITY, f64::NEG_INFINITY, 0.0, count)
            }
        }
    }
}

impl ReduceFunctionTrait for ArrowReduceFunction {
    fn create_accumulator(&self) -> Accumulator {
        Accumulator::new(f64::INFINITY, f64::NEG_INFINITY, 0.0, 0)     
    }
    
    fn update_accumulator(&self, accumulator: &mut Accumulator, message: &KeyedMessage) {
        let record_batch = &message.base.record_batch;
        let schema = record_batch.schema();
        
        // Find the column to aggregate
        if let Some((idx, _)) = schema.column_with_name(&self.column_name) {
            let array = record_batch.column(idx);
            
            // Compute aggregations for this batch
            let (min_val, max_val, sum_val, count) = self.compute_aggregations(array);
            
            // Update the accumulator with these values
            accumulator.update(min_val, max_val, sum_val, count);
        } else {
            panic!("Column '{}' not found in batch", self.column_name)
        }
    }
    
    fn get_result(&self, accumulator: &Accumulator) -> AggregationResult {
        AggregationResult {
            min: accumulator.min,
            max: accumulator.max,
            sum: accumulator.sum,
            count: accumulator.count as f64,
            average: accumulator.average(),
        }
    }
}

/// Enum to select between different reduce function implementations
#[derive(Debug, Clone)]
pub enum ReduceFunction {
    Custom(CustomReduceFunction),
    Arrow(ArrowReduceFunction),
}

impl ReduceFunctionTrait for ReduceFunction {
    fn create_accumulator(&self) -> Accumulator {
        match self {
            ReduceFunction::Custom(function) => function.create_accumulator(),
            ReduceFunction::Arrow(function) => function.create_accumulator(),
        }
    }
    
    fn update_accumulator(&self, accumulator: &mut Accumulator, message: &KeyedMessage) {
        match self {
            ReduceFunction::Custom(function) => function.update_accumulator(accumulator, message),
            ReduceFunction::Arrow(function) => function.update_accumulator(accumulator, message),
        }
    }
    
    fn get_result(&self, accumulator: &Accumulator) -> AggregationResult {
        match self {
            ReduceFunction::Custom(function) => function.get_result(accumulator),
            ReduceFunction::Arrow(function) => function.get_result(accumulator),
        }
    }
}

impl ReduceFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: ReduceFunctionTrait + 'static,
    {
        Self::Custom(CustomReduceFunction::new(function))
    }
    
    pub fn new_arrow_reduce(column_name: String) -> Self {
        Self::Arrow(ArrowReduceFunction::new(column_name))
    }
}

#[async_trait]
impl FunctionTrait for ReduceFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Default implementation does nothing
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
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn create_test_keyed_batch(values: Vec<f64>, key_value: i32) -> KeyedMessage {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("value", DataType::Float64, false),
        ]));
        
        let value_array = Float64Array::from(values);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(value_array)]
        ).unwrap();
        
        let base_message = BaseMessage::new(None, record_batch, None);
        
        // Create key batch with a single row
        let key_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("key", DataType::Int32, false),
        ]));
        
        let key_array = Int32Array::from(vec![key_value]);
        
        let key_batch = RecordBatch::try_new(
            key_schema,
            vec![Arc::new(key_array)]
        ).unwrap();
        
        let key = Key::new(key_batch).unwrap();
        
        KeyedMessage::new(base_message, key)
    }

    #[tokio::test]
    async fn test_arrow_reduce_function() {
        // Test data in multiple batches with the same key
        let initial_message = create_test_keyed_batch(vec![10.0, 5.0, 20.0], 1);
        let second_message = create_test_keyed_batch(vec![15.0, 3.0, 25.0], 1);
        let third_message = create_test_keyed_batch(vec![40.0, 50.0], 1);
        
        // Create reducer
        let reducer = ArrowReduceFunction::new("value".to_string());
        
        // Initialize accumulator with first message
        let mut acc = reducer.create_accumulator();
        reducer.update_accumulator(&mut acc, &initial_message);
        
        // Check initial values
        assert_eq!(acc.min, 5.0);
        assert_eq!(acc.max, 20.0);
        assert_eq!(acc.sum, 35.0);  // 10 + 5 + 20
        assert_eq!(acc.count, 3);
        assert_eq!(acc.average(), 35.0 / 3.0);
        
        // Update with second message
        reducer.update_accumulator(&mut acc, &second_message);
        
        // Verify after second message
        assert_eq!(acc.min, 3.0);  // Min from both messages
        assert_eq!(acc.max, 25.0);  // Max from both messages
        assert_eq!(acc.sum, 78.0);  // 35 + 15 + 3 + 25
        assert_eq!(acc.count, 6);   // 3 + 3
        assert_eq!(acc.average(), 78.0 / 6.0);
        
        // Update with third message
        reducer.update_accumulator(&mut acc, &third_message);
        
        // Verify after third message
        assert_eq!(acc.min, 3.0);   // Min across all messages
        assert_eq!(acc.max, 50.0);  // Max across all messages
        assert_eq!(acc.sum, 168.0); // 78 + 40 + 50
        assert_eq!(acc.count, 8);   // 6 + 2
        assert_eq!(acc.average(), 168.0 / 8.0);
        
        // Get results and verify all aggregation types
        let result = reducer.get_result(&acc);
        assert_eq!(result.min, 3.0);
        assert_eq!(result.max, 50.0);
        assert_eq!(result.sum, 168.0);
        assert_eq!(result.count, 8.0);
        assert_eq!(result.average, 168.0 / 8.0);

        let key = initial_message.key();

        // Test min aggregation
        let min_extractor = SingleAggregationResultExtractor::new(AggregationType::Min, "min_value".to_string());
        let min_message = min_extractor.extract_result(&key, &result, None, None);
        let min_value = min_message.record_batch().column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert_eq!(min_value, 3.0);
        
        // Test max aggregation
        let max_extractor = SingleAggregationResultExtractor::new(AggregationType::Max, "max_value".to_string());
        let max_message = max_extractor.extract_result(&key, &result, None, None);
        let max_value = max_message.record_batch().column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert_eq!(max_value, 50.0);
        
        // Test sum aggregation
        let sum_extractor = SingleAggregationResultExtractor::new(AggregationType::Sum, "sum_value".to_string());
        let sum_message = sum_extractor.extract_result(&key, &result, None, None);
        let sum_value = sum_message.record_batch().column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert_eq!(sum_value, 168.0);
        
        // Test count aggregation
        let count_extractor = SingleAggregationResultExtractor::new(AggregationType::Count, "count_value".to_string());
        let count_message = count_extractor.extract_result(&key, &result, None, None);
        let count_value = count_message.record_batch().column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert_eq!(count_value, 8.0);
    }

    #[tokio::test]
    async fn test_arrow_reduce_function_different_types() {
        // Test with float values
        let float_message = create_test_keyed_batch(vec![10.0, 5.0, 20.0], 1);

        // Test with string values
        let string_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));
        let string_array = StringArray::from(vec!["a", "b", "c"]);
        let string_batch = RecordBatch::try_new(
            string_schema.clone(),
            vec![Arc::new(string_array)]
        ).unwrap();
        let string_message = KeyedMessage::new(
            BaseMessage::new(None, string_batch, None),
            Key::new(RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))]
            ).unwrap()).unwrap(),
        );

        // Test float column
        let reducer = ArrowReduceFunction::new("value".to_string());
        
        // Test float message
        let mut acc = reducer.create_accumulator();
        reducer.update_accumulator(&mut acc, &float_message);
        
        assert_eq!(acc.min, 5.0);
        assert_eq!(acc.max, 20.0);
        assert_eq!(acc.sum, 35.0);
        assert_eq!(acc.count, 3);
        assert_eq!(acc.average(), 35.0 / 3.0);

        // Test string message (should return default values with correct count)
        let mut acc = reducer.create_accumulator();
        reducer.update_accumulator(&mut acc, &string_message);

        assert_eq!(acc.min, f64::INFINITY);
        assert_eq!(acc.max, f64::NEG_INFINITY);
        assert_eq!(acc.sum, 0.0);
        assert_eq!(acc.count, 3);  // Count should still be correct
        assert_eq!(acc.average(), 0.0);

        // Test accumulation with different types
        let mut acc = reducer.create_accumulator();
        
        // Accumulate float message
        reducer.update_accumulator(&mut acc, &float_message);
        assert_eq!(acc.min, 5.0);
        assert_eq!(acc.max, 20.0);
        assert_eq!(acc.sum, 35.0);
        assert_eq!(acc.count, 3);
        assert_eq!(acc.average(), 35.0 / 3.0);

        // Accumulate string message (should not affect float values)
        reducer.update_accumulator(&mut acc, &string_message);
        assert_eq!(acc.min, 5.0);  // Should not change
        assert_eq!(acc.max, 20.0); // Should not change
        assert_eq!(acc.sum, 35.0); // Should not change
        assert_eq!(acc.count, 6);  // Should add string batch count # TODO is this ok?
        assert_eq!(acc.average(), 35.0 / 6.0);  // Average should be affected by count
    }
} 