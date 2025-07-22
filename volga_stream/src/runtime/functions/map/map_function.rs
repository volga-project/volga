use std::sync::Arc;
use async_trait::async_trait;
use crate::common::message::Message;
use crate::runtime::functions::map::{FilterFunction, ProjectionFunction};
use anyhow::Result;
use std::fmt;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

#[async_trait]
pub trait MapFunctionTrait: Send + Sync + fmt::Debug {
    fn map(&self, message: Message) -> Result<Message>;
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
    fn map(&self, message: Message) -> Result<Message> {
        self.function.map(message)
    }
}

#[derive(Debug, Clone)]
pub enum MapFunction {
    Custom(CustomMapFunction),
    Filter(FilterFunction),
    Projection(ProjectionFunction)
}

impl fmt::Display for MapFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MapFunction::Custom(_) => write!(f, "Custom"),
            MapFunction::Filter(_) => write!(f, "Filter"),
            MapFunction::Projection(_) => write!(f, "Projection"),
        }
    }
}

impl MapFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: MapFunctionTrait + 'static,
    {
        Self::Custom(CustomMapFunction::new(function))
    }

    pub fn map(&self, message: Message) -> Result<Message> {
        match self {
            MapFunction::Custom(function) => function.map(message),
            MapFunction::Filter(function) => function.map(message),
            MapFunction::Projection(function) => function.map(message)
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
        fn map(&self, message: Message) -> Result<Message> {
            let record_batch = message.record_batch();
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
            
            Ok(Message::new(message.upstream_vertex_id(), new_batch, message.ingest_timestamp()))
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
        
        let message = Message::new(None, record_batch, None);
        
        // Create and execute map function
        let map_function = MapFunction::new_custom(TestMapFunction);
        let result = map_function.map(message).unwrap();
        
        // Verify result
        let result_array = result.record_batch().column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.values(), &[2, 4, 6, 8, 10]);
    }
}