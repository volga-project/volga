use std::sync::Arc;
use async_trait::async_trait;
use crate::common::data_batch::{DataBatch, KeyedDataBatch, BaseDataBatch};
use crate::common::Key;
use anyhow::Result;
use std::fmt;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::compute;
use arrow::compute::kernels::aggregate::{sum, min, max};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::function_trait::FunctionTrait;
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

#[async_trait]
pub trait ReduceFunctionTrait: Send + Sync + fmt::Debug {
    async fn create_accumulator(&self, key_batch: &KeyedDataBatch) -> Result<Accumulator>;
    
    async fn update_accumulator(&self, accumulator: &mut Accumulator, batch: &KeyedDataBatch) -> Result<()>;
    
    async fn get_result(&self, accumulator: &Accumulator) -> Result<AggregationResult>;
}

/// Trait for extracting final results from aggregated data
#[async_trait]
pub trait AggregationResultExtractorTrait: Send + Sync + fmt::Debug {
    async fn extract_result(&self, key: &Key, result: &AggregationResult) -> Result<DataBatch>;
}

/// Default implementation of ResultExtractor that includes all aggregation values
#[derive(Debug, Clone)]
pub struct AllAggregationsResultExtractor;

#[async_trait]
impl AggregationResultExtractorTrait for AllAggregationsResultExtractor {
    async fn extract_result(&self, key: &Key, result: &AggregationResult) -> Result<DataBatch> {
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
        )?;
        
        Ok(DataBatch::KeyedBatch(KeyedDataBatch::new(
            BaseDataBatch::new(None, record_batch),
            key.clone(),
        )))
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

#[async_trait]
impl AggregationResultExtractorTrait for SingleAggregationResultExtractor {
    async fn extract_result(&self, key: &Key, result: &AggregationResult) -> Result<DataBatch> {
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
        )?;
        
        Ok(DataBatch::KeyedBatch(KeyedDataBatch::new(
            BaseDataBatch::new(None, record_batch),
            key.clone(),
        )))
    }
}

/// Wrapper enum for different result extractors
#[derive(Debug, Clone)]
pub enum AggregationResultExtractor {
    All(AllAggregationsResultExtractor),
    Single(SingleAggregationResultExtractor),
    Custom(Arc<dyn AggregationResultExtractorTrait>),
}

#[async_trait]
impl AggregationResultExtractorTrait for AggregationResultExtractor {
    async fn extract_result(&self, key: &Key, result: &AggregationResult) -> Result<DataBatch> {
        match self {
            AggregationResultExtractor::All(e) => e.extract_result(key, result).await,
            AggregationResultExtractor::Single(e) => e.extract_result(key, result).await,
            AggregationResultExtractor::Custom(e) => e.extract_result(key, result).await,
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

#[async_trait]
impl ReduceFunctionTrait for CustomReduceFunction {
    async fn create_accumulator(&self, key_batch: &KeyedDataBatch) -> Result<Accumulator> {
        self.function.create_accumulator(key_batch).await
    }
    
    async fn update_accumulator(&self, accumulator: &mut Accumulator, batch: &KeyedDataBatch) -> Result<()> {
        self.function.update_accumulator(accumulator, batch).await
    }
    
    async fn get_result(&self, accumulator: &Accumulator) -> Result<AggregationResult> {
        self.function.get_result(accumulator).await
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
    
    fn compute_aggregations(&self, array: &ArrayRef) -> Result<(f64, f64, f64, i64)> {
        let array_f64 = compute::cast(array, &DataType::Float64)?;
        
        let float_array = array_f64.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
        
        let count = float_array.len() - float_array.null_count();
        
        if count == 0 {
            return Ok((f64::INFINITY, f64::NEG_INFINITY, 0.0, 0));
        }
        
        let min_val = min(float_array)
            .ok_or_else(|| anyhow::anyhow!("Failed to compute min"))?;
        let max_val = max(float_array)
            .ok_or_else(|| anyhow::anyhow!("Failed to compute max"))?;
        let sum_val = sum(float_array)
            .ok_or_else(|| anyhow::anyhow!("Failed to compute sum"))?;
        
        Ok((min_val, max_val, sum_val, count as i64))
    }
}

#[async_trait]
impl ReduceFunctionTrait for ArrowReduceFunction {
    async fn create_accumulator(&self, key_batch: &KeyedDataBatch) -> Result<Accumulator> {
        let batch = &key_batch.base.record_batch;
        let schema = batch.schema();
        
        // Find the column to aggregate
        if let Some((idx, _)) = schema.column_with_name(&self.column_name) {
            let array = batch.column(idx);
            
            // Compute initial aggregations
            let (min_val, max_val, sum_val, count) = self.compute_aggregations(array)?;
            
            // Create an accumulator with the initial values
            Ok(Accumulator::new(min_val, max_val, sum_val, count))
        } else {
            Err(anyhow::anyhow!("Column '{}' not found in schema", self.column_name))
        }
    }
    
    async fn update_accumulator(&self, accumulator: &mut Accumulator, batch: &KeyedDataBatch) -> Result<()> {
        let record_batch = &batch.base.record_batch;
        let schema = record_batch.schema();
        
        // Find the column to aggregate
        if let Some((idx, _)) = schema.column_with_name(&self.column_name) {
            let array = record_batch.column(idx);
            
            // Compute aggregations for this batch
            let (min_val, max_val, sum_val, count) = self.compute_aggregations(array)?;
            
            // Update the accumulator with these values
            accumulator.update(min_val, max_val, sum_val, count);
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("Column '{}' not found in batch", self.column_name))
        }
    }
    
    async fn get_result(&self, accumulator: &Accumulator) -> Result<AggregationResult> {
        Ok(AggregationResult {
            min: accumulator.min,
            max: accumulator.max,
            sum: accumulator.sum,
            count: accumulator.count as f64,
            average: accumulator.average(),
        })
    }
}

/// Enum to select between different reduce function implementations
#[derive(Debug, Clone)]
pub enum ReduceFunction {
    Custom(CustomReduceFunction),
    Arrow(ArrowReduceFunction),
}

#[async_trait]
impl ReduceFunctionTrait for ReduceFunction {
    async fn create_accumulator(&self, key_batch: &KeyedDataBatch) -> Result<Accumulator> {
        match self {
            ReduceFunction::Custom(function) => function.create_accumulator(key_batch).await,
            ReduceFunction::Arrow(function) => function.create_accumulator(key_batch).await,
        }
    }
    
    async fn update_accumulator(&self, accumulator: &mut Accumulator, batch: &KeyedDataBatch) -> Result<()> {
        match self {
            ReduceFunction::Custom(function) => function.update_accumulator(accumulator, batch).await,
            ReduceFunction::Arrow(function) => function.update_accumulator(accumulator, batch).await,
        }
    }
    
    async fn get_result(&self, accumulator: &Accumulator) -> Result<AggregationResult> {
        match self {
            ReduceFunction::Custom(function) => function.get_result(accumulator).await,
            ReduceFunction::Arrow(function) => function.get_result(accumulator).await,
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
    
    async fn finish(&mut self) -> Result<()> {
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
    use arrow::array::{Int32Array, Float64Array};
    use arrow::datatypes::{Field, DataType};
    use crate::common::data_batch::BaseDataBatch;

    fn create_test_keyed_batch(values: Vec<f64>, key_value: i32) -> KeyedDataBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("value", DataType::Float64, false),
        ]));
        
        let value_array = Float64Array::from(values);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(value_array)]
        ).unwrap();
        
        let base_batch = BaseDataBatch::new(None, record_batch);
        
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
        
        KeyedDataBatch::new(base_batch, key)
    }

    #[tokio::test]
    async fn test_arrow_reduce_function() {
        // Test data in multiple batches with the same key
        let initial_batch = create_test_keyed_batch(vec![10.0, 5.0, 20.0], 1);
        let second_batch = create_test_keyed_batch(vec![15.0, 3.0, 25.0], 1);
        let third_batch = create_test_keyed_batch(vec![40.0, 50.0], 1);
        
        // Create reducer
        let reducer = ArrowReduceFunction::new("value".to_string());
        
        // Initialize accumulator with first batch
        let mut acc = reducer.create_accumulator(&initial_batch).await.unwrap();
        
        // Check initial values
        assert_eq!(acc.min, 5.0);
        assert_eq!(acc.max, 20.0);
        assert_eq!(acc.sum, 35.0);  // 10 + 5 + 20
        assert_eq!(acc.count, 3);
        assert_eq!(acc.average(), 35.0 / 3.0);
        
        // Update with second batch
        reducer.update_accumulator(&mut acc, &second_batch).await.unwrap();
        
        // Verify after second batch
        assert_eq!(acc.min, 3.0);  // Min from both batches
        assert_eq!(acc.max, 25.0);  // Max from both batches
        assert_eq!(acc.sum, 78.0);  // 35 + 15 + 3 + 25
        assert_eq!(acc.count, 6);   // 3 + 3
        assert_eq!(acc.average(), 78.0 / 6.0);
        
        // Update with third batch
        reducer.update_accumulator(&mut acc, &third_batch).await.unwrap();
        
        // Verify after third batch
        assert_eq!(acc.min, 3.0);   // Min across all batches
        assert_eq!(acc.max, 50.0);  // Max across all batches
        assert_eq!(acc.sum, 168.0); // 78 + 40 + 50
        assert_eq!(acc.count, 8);   // 6 + 2
        assert_eq!(acc.average(), 168.0 / 8.0);
        
        // Get results and verify all aggregation types
        let result = reducer.get_result(&acc).await.unwrap();
        assert_eq!(result.min, 3.0);
        assert_eq!(result.max, 50.0);
        assert_eq!(result.sum, 168.0);
        assert_eq!(result.count, 8.0);
        assert_eq!(result.average, 168.0 / 8.0);
    }
} 