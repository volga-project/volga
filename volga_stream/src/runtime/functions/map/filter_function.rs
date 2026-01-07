use std::sync::Arc;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::physical_plan::PhysicalExpr;
use crate::common::message::Message;
use anyhow::Result;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::map::MapFunctionTrait;
use std::any::Any;

use datafusion::logical_expr::Expr;
use datafusion::execution::context::SessionContext;
use arrow::compute::filter_record_batch;
use arrow::array::BooleanArray;

// TODO use Datafusion's FilterExec
#[derive(Clone)]
pub struct FilterFunction {
    _schema: DFSchemaRef,
    predicate: Expr,
    _session_context: SessionContext,
    physical_expr: Arc<dyn PhysicalExpr>,
}

impl std::fmt::Debug for FilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterFunction")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl FilterFunction {
    pub fn new(
        schema: DFSchemaRef,
        predicate: Expr, 
        session_context: SessionContext,
    ) -> Self {
        let physical_expr = session_context.create_physical_expr(predicate.clone(), &schema).unwrap();
        
        Self { 
            _schema: schema,
            predicate, 
            _session_context: session_context,
            physical_expr,
        }
    }
}

#[async_trait]
impl MapFunctionTrait for FilterFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let record_batch = message.record_batch();
        // Convert logical expression to physical expression
        
        let result = self.physical_expr
            .evaluate(record_batch)
            .and_then(|v| v.into_array(record_batch.num_rows()))
            .and_then(|array| {
                let boolean_array = array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| datafusion::common::DataFusionError::Plan("Cannot create filter_array from non-boolean predicates".to_string()))?;
                
                Ok(filter_record_batch(record_batch, boolean_array)?)
            })?;
        
        let new_message = Message::new(
            message.upstream_vertex_id(),
            result,
            message.ingest_timestamp(),
            message.get_extras(),
        );
        Ok(new_message)
    }
}

#[async_trait]
impl FunctionTrait for FilterFunction {
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
    use arrow::array::{Int32Array, BooleanArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::common::DFSchema;
    use std::sync::Arc;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr;

    #[tokio::test]
    async fn test_filter_function() {
        // Create a test record batch with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("score", arrow::datatypes::DataType::Float64, false),
        ]));
        
        let value_array = Int32Array::from(vec![3, 7, 2, 8, 1]);
        let name_array = arrow::array::StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let score_array = arrow::array::Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(value_array), Arc::new(name_array), Arc::new(score_array)]
        ).unwrap();
        
        // Create a DataFusion expression: value > 5
        let ctx = SessionContext::new();
        let predicate = logical_expr::col("value").gt(logical_expr::lit(5));
        
        // Test 1: Filter only (no projection)
        let filter_func = FilterFunction::new(DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()), predicate.clone(), ctx.clone());
        let message = Message::new(None, batch.clone(), None, None);
        
        let result = filter_func.map(message).unwrap();
        let filtered_batch = result.record_batch();
        
        // Should have 2 rows (7 and 8) with all 3 columns
        assert_eq!(filtered_batch.num_rows(), 2);
        assert_eq!(filtered_batch.num_columns(), 3);
        
        let filtered_value_array = filtered_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(filtered_value_array.value(0), 7);
        assert_eq!(filtered_value_array.value(1), 8);
    }
} 