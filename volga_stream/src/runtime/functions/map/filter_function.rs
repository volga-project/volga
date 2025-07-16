use std::sync::Arc;
use async_trait::async_trait;
use crate::common::message::Message;
use anyhow::Result;
use std::fmt;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::map::MapFunctionTrait;
use std::any::Any;

use datafusion::logical_expr::Expr;
use datafusion::execution::context::SessionContext;
use arrow::record_batch::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::array::BooleanArray;

#[derive(Clone)]
pub struct FilterFunction {
    predicate: Expr,
    session_context: SessionContext,
    projection: Option<Vec<usize>>,
}

impl std::fmt::Debug for FilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterFunction")
            .field("predicate", &self.predicate)
            .field("session_context", &"SessionContext")
            .field("projection", &self.projection)
            .finish()
    }
}

impl FilterFunction {
    pub fn new(
        predicate: Expr, 
        session_context: SessionContext, 
        projection: Option<Vec<usize>>
    ) -> Self {
        Self { 
            predicate, 
            session_context,
            projection,
        }
    }
}

#[async_trait]
impl MapFunctionTrait for FilterFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let record_batch = message.record_batch();
        // Convert logical expression to physical expression
        let df_schema = datafusion::common::DFSchema::try_from(record_batch.schema().clone())?;
        let physical_expr = self.session_context.create_physical_expr(self.predicate.clone(), &df_schema)?;
        
        let result = physical_expr
            .evaluate(record_batch)
            .and_then(|v| v.into_array(record_batch.num_rows()))
            .and_then(|array| {
                let boolean_array = array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| datafusion::common::DataFusionError::Plan("Cannot create filter_array from non-boolean predicates".to_string()))?;
                
                Ok(match &self.projection {
                    None => filter_record_batch(record_batch, boolean_array)?,
                    Some(projection) => {
                        let projected_columns = projection
                            .iter()
                            .map(|i| Arc::clone(record_batch.column(*i)))
                            .collect();
                        let projected_schema = Arc::new(arrow::datatypes::Schema::new(
                            projection
                                .iter()
                                .map(|&i| record_batch.schema().field(i).clone())
                                .collect::<Vec<_>>()
                        ));
                        let projected_batch = RecordBatch::try_new(projected_schema, projected_columns)?;
                        filter_record_batch(&projected_batch, boolean_array)?
                    }
                })
            })?;
        
        let new_message = Message::new(
            message.upstream_vertex_id(),
            result,
            message.ingest_timestamp(),
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
        let filter_func = FilterFunction::new(predicate.clone(), ctx.clone(), None);
        let message = Message::new(None, batch.clone(), None);
        
        let result = filter_func.map(message).unwrap();
        let filtered_batch = result.record_batch();
        
        // Should have 2 rows (7 and 8) with all 3 columns
        assert_eq!(filtered_batch.num_rows(), 2);
        assert_eq!(filtered_batch.num_columns(), 3);
        
        let filtered_value_array = filtered_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(filtered_value_array.value(0), 7);
        assert_eq!(filtered_value_array.value(1), 8);
        
        // Test 2: Filter with projection (select only value and score columns)
        let projection = Some(vec![0, 2]); // value and score columns
        let filter_func_with_proj = FilterFunction::new(predicate, ctx, projection);
        let message = Message::new(None, batch, None);
        
        let result = filter_func_with_proj.map(message).unwrap();
        let filtered_batch = result.record_batch();
        
        // Should have 2 rows with only 2 columns (value and score)
        assert_eq!(filtered_batch.num_rows(), 2);
        assert_eq!(filtered_batch.num_columns(), 2);
        
        let filtered_value_array = filtered_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let filtered_score_array = filtered_batch.column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        
        assert_eq!(filtered_value_array.value(0), 7);
        assert_eq!(filtered_value_array.value(1), 8);
        assert_eq!(filtered_score_array.value(0), 2.2);
        assert_eq!(filtered_score_array.value(1), 4.4);
    }
} 