use std::sync::Arc;
use async_trait::async_trait;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::fmt;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

#[async_trait]
pub trait MapFunctionTrait: Send + Sync + fmt::Debug {
    async fn map(&self, batch: DataBatch) -> Result<DataBatch>;
}

#[derive(Debug, Clone)]
pub struct CustomMapFunction {
    function: Arc<dyn MapFunctionTrait>,
}

impl CustomMapFunction {
    pub fn new<F>(function: F) -> Self 
    where
        F: MapFunctionTrait + 'static,
    {
        Self {
            function: Arc::new(function),
        }
    }
}

#[async_trait]
impl MapFunctionTrait for CustomMapFunction {
    async fn map(&self, batch: DataBatch) -> Result<DataBatch> {
        self.function.map(batch).await
    }
}

#[derive(Debug, Clone)]
pub enum MapFunction {
    Custom(CustomMapFunction),
}

impl MapFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: MapFunctionTrait + 'static,
    {
        Self::Custom(CustomMapFunction::new(function))
    }

    pub async fn map(&self, batch: DataBatch) -> Result<DataBatch> {
        match self {
            MapFunction::Custom(function) => function.map(batch).await,
        }
    }
}

#[async_trait]
impl FunctionTrait for MapFunction {
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
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestMapFunction;

    #[async_trait]
    impl MapFunctionTrait for TestMapFunction {
        async fn map(&self, batch: DataBatch) -> Result<DataBatch> {
            let record_batch = batch.record_batch();
            let schema = record_batch.schema();
            
            // Get the input array
            let input_array = record_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            
            // Create output array (multiply each value by 2)
            let output_array: Int32Array = input_array.iter()
                .map(|v| v.map(|x| x * 2))
                .collect();
            
            // Create new batch with same schema
            let new_batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(output_array)]
            )?;
            
            Ok(DataBatch::new(batch.upstream_vertex_id(), new_batch))
        }
    }

    #[tokio::test]
    async fn test_custom_map_function() {
        // Create test input
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]));
        
        let input_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let record_batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(input_array)]
        ).unwrap();
        
        let batch = DataBatch::new(None, record_batch);
        
        // Create and execute map function
        let map_function = MapFunction::new_custom(TestMapFunction);
        let result = map_function.map(batch).await.unwrap();
        
        // Verify result
        let result_array = result.record_batch().column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.values(), &[2, 4, 6, 8, 10]);
    }
}

// pub fn create_map_function(config: ...) -> MapFunction { ... } 