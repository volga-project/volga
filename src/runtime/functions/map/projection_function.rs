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
use arrow::record_batch::RecordBatch;

// TODO use Datafusion's ProjectionExec
#[derive(Clone)]
pub struct ProjectionFunction {
    _in_schema: DFSchemaRef,
    out_schema: DFSchemaRef,
    exprs: Vec<Expr>,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    _session_context: SessionContext,
}

impl std::fmt::Debug for ProjectionFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionFunction")
            .field("exprs", &self.exprs)
            .finish()
    }
}

impl ProjectionFunction {
    pub fn new(
        in_schema: DFSchemaRef,
        out_schema: DFSchemaRef,
        exprs: Vec<Expr>, 
        session_context: SessionContext
    ) -> Self {
        let physical_exprs = exprs
            .iter()
            .map(|e| {
                session_context.create_physical_expr(e.clone(), &in_schema).expect("unable to create physical expr")
            })
            .collect::<Vec<_>>();
        
        Self { 
            _in_schema: in_schema,
            out_schema,
            exprs, 
            physical_exprs,
            _session_context: session_context,
        }
    }

    pub fn out_schema(&self) -> DFSchemaRef {
        self.out_schema.clone()
    }
}

#[async_trait]
impl MapFunctionTrait for ProjectionFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let record_batch = message.record_batch();
        
        // Use pre-computed physical expressions (created with qualified schema in constructor)
        let projected_arrays = self.physical_exprs.iter()
            .map(|physical_expr| {
                physical_expr
                    .evaluate(record_batch)
                    .expect("Failed to evaluate physical expression")
                    .into_array(record_batch.num_rows())
                    .expect("Failed to convert to array")
            })
            .collect::<Vec<_>>();
        
        // Create new record batch with projected columns
        let projected_batch = RecordBatch::try_new(
            self.out_schema.inner().clone(),
            projected_arrays
        ).expect("Failed to create new record batch");
        
        let new_message = Message::new(
            message.upstream_vertex_id(),
            projected_batch,
            message.ingest_timestamp(),
            message.get_extras(),
        );
        Ok(new_message)
    }
}

#[async_trait]
impl FunctionTrait for ProjectionFunction {
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
    use arrow::array::{Int32Array, StringArray, Float64Array};
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use datafusion::common::DFSchema;
    use std::sync::Arc;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr;

    #[tokio::test]
    async fn test_projection_function() {
        // Create a test record batch with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("score", arrow::datatypes::DataType::Float64, false),
        ]));
        
        let value_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let score_array = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(value_array), Arc::new(name_array), Arc::new(score_array)]
        ).unwrap();
        
        // Create DataFusion expressions: select value and name columns
        let ctx = SessionContext::new();
        let expressions = vec![
            logical_expr::col("value"),
            logical_expr::col("name"),
        ];

        let out_schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        
        // Test projection
        let projection_func = ProjectionFunction::new(
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()), 
            DFSchemaRef::from(DFSchema::try_from(out_schema.clone()).unwrap()), 
            expressions, 
            ctx
        );
        let message = Message::new(None, batch, None, None);
        
        let result = projection_func.map(message).unwrap();
        let projected_batch = result.record_batch();
        
        // Should have 5 rows with only 2 columns (value and name)
        assert_eq!(projected_batch.num_rows(), 5);
        assert_eq!(projected_batch.num_columns(), 2);
        
        let projected_value_array = projected_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let projected_name_array = projected_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        
        assert_eq!(projected_value_array.values(), &[1, 2, 3, 4, 5]);
        assert_eq!(projected_name_array.iter().collect::<Vec<_>>(), vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);
    }

    #[tokio::test]
    async fn test_projection_with_literal() {
        // Create a test record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
        ]));
        
        let value_array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(value_array)]).unwrap();
        
        // Create expressions: select value column and a literal
        let ctx = SessionContext::new();
        let expressions = vec![
            logical_expr::col("value"),
            logical_expr::lit(42),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Int32, false),
            Field::new("lit", arrow::datatypes::DataType::Int32, false),
        ]));
        
        let projection_func = ProjectionFunction::new(
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()), 
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()), 
            expressions, 
            ctx
        );
        let message = Message::new(None, batch, None, None);
        
        let result = projection_func.map(message).unwrap();
        let projected_batch = result.record_batch();
        
        // Should have 3 rows with 2 columns (value and literal)
        assert_eq!(projected_batch.num_rows(), 3);
        assert_eq!(projected_batch.num_columns(), 2);
        
        let projected_value_array = projected_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let literal_array = projected_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        
        assert_eq!(projected_value_array.values(), &[1, 2, 3]);
        assert_eq!(literal_array.values(), &[42, 42, 42]);
    }
}
