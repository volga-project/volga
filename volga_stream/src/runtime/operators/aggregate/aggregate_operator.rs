use std::collections::HashMap;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use datafusion::common::Result;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::{Accumulator, ExecutionPlan};
use datafusion::physical_plan::aggregates::AggregateExec;
use async_trait::async_trait;
use anyhow::Result as AnyhowResult;
use crate::runtime::operators::operator::{OperatorTrait, OperatorBase, OperatorType, OperatorConfig};
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::{Message, Key};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AggregateConfig {
    pub aggregate_exec: AggregateExec,
}

#[derive(Debug)]
pub struct AggregateOperator {
    base: OperatorBase,
    aggregate_exec: Arc<AggregateExec>,  // DataFusion's AggregateExec
    accumulators: HashMap<Key, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)>, // (group_values, accumulators)
}

impl AggregateOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let aggregate_confg = match config.clone() {
            OperatorConfig::AggregateConfig(aggregate_config) => aggregate_config,
            _ => panic!("Expected AggregateConfig, got {:?}", config),
        };

        Self {
            base: OperatorBase::new(OperatorConfig::AggregateConfig(aggregate_confg.clone())),
            aggregate_exec: Arc::new(aggregate_confg.aggregate_exec),
            accumulators: HashMap::new(),
        }
    }
    
    fn create_accumulators(&self) -> Vec<Box<dyn Accumulator>> {
        self.aggregate_exec.aggr_expr()
            .iter()
            .map(|expr| {
                expr.create_accumulator().expect("should be able to create accumulator")
            })
            .collect()
    }
    
    #[allow(dead_code)]
    fn create_result_batch(&self, group_values: Vec<ArrayRef>, mut accumulators: Vec<Box<dyn Accumulator>>) -> Result<RecordBatch> {
        let mut columns = Vec::new();
        
        // 1. Add group by columns (use stored values)
        for group_array in group_values {
            columns.push(group_array);
        }
        
        // 2. Add aggregate result columns
        for accumulator in &mut accumulators {
            let result = accumulator.evaluate()?;
            let result_array = result.to_array_of_size(1)?;
            columns.push(result_array);
        }
        
        // TODO output schema should come from aggregateexec
        let output_physical_schema = self.aggregate_exec.schema();
        Ok(RecordBatch::try_new(output_physical_schema.clone(), columns)?)
    }
    
    fn emit_all_accumulators(&mut self) -> Vec<Message> {
        let mut messages = Vec::new();
        let accumulators: Vec<_> = self.accumulators.drain().collect();
        
        for (key, (group_values, accumulators)) in accumulators {
            let batch_result = {
                let mut columns = Vec::new();
                
                // 1. Add group by columns (use stored values)
                for group_array in group_values {
                    columns.push(group_array);
                }
                
                // 2. Add aggregate result columns
                let mut accs = accumulators;
                for accumulator in &mut accs {
                    if let Ok(result) = accumulator.evaluate() {
                        if let Ok(result_array) = result.to_array_of_size(1) {
                            columns.push(result_array);
                        }
                    }
                }
                
                // TODO output schema should come from aggregateexec
                let output_physical_schema = self.aggregate_exec.schema();
                RecordBatch::try_new(output_physical_schema.clone(), columns)
            };
            
            if let Ok(batch) = batch_result {
                messages.push(Message::new_keyed(
                    None,
                    batch,
                    key,
                    None
                ));
            }
        }
        
        messages
    }
}

#[async_trait]
impl OperatorTrait for AggregateOperator {
    async fn open(&mut self, context: &RuntimeContext) -> AnyhowResult<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> AnyhowResult<()> {
        self.base.close().await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        match message {
            Message::Keyed(keyed_message) => {
                let key = keyed_message.key().clone();
                
                // Extract group by values from the batch using output_exprs
                let group_by_result = self.aggregate_exec.group_expr()
                    .output_exprs()
                    .iter()
                    .map(|expr| expr.evaluate(&keyed_message.base.record_batch))
                    .collect::<Result<Vec<_>>>();
                
                let group_by_values = match group_by_result {
                    Ok(values) => {
                        // Convert ColumnarValue to ArrayRef
                        values
                            .into_iter()
                            .filter_map(|v| match v {
                                ColumnarValue::Array(a) => Some(a),
                                ColumnarValue::Scalar(s) => {
                                    let batch_size = keyed_message.base.record_batch.num_rows();
                                    s.to_array_of_size(batch_size).ok()
                                }
                            })
                            .collect::<Vec<ArrayRef>>()
                    },
                    Err(_) => return None, // Skip on error
                };
                
                // Get or create accumulators for this key
                let key_clone = key.clone();
                if !self.accumulators.contains_key(&key_clone) {
                    let accs = self.create_accumulators();
                    self.accumulators.insert(key_clone.clone(), (group_by_values.clone(), accs));
                }
                let (_stored_group_values, accumulators) = self.accumulators
                    .get_mut(&key_clone)
                    .expect("Accumulators should exist after insert");
                
                // Update accumulators
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    if let Some(aggr_expr) = self.aggregate_exec.aggr_expr().get(i) {
                        let values = aggr_expr
                        .expressions()
                        .iter()
                        .map(|expr| expr.evaluate(&keyed_message.base.record_batch).expect("should be able to evaluate expression"))
                            .collect::<Vec<_>>();
                        
                        // Convert ColumnarValue to ArrayRef
                        let arrays: Vec<ArrayRef> = values
                            .into_iter()
                            .filter_map(|v| match v {
                                ColumnarValue::Array(a) => Some(a),
                                ColumnarValue::Scalar(s) => {
                                    // Convert scalar to array with batch size
                                    let batch_size = keyed_message.base.record_batch.num_rows();
                                    s.to_array_of_size(batch_size).ok()
                                }
                            })
                            .collect();
                        
                        if !arrays.is_empty() {
                            let _ = accumulator.update_batch(&arrays);
                        }
                        
                    }
                }
                
                // No immediate emission - wait for watermark
                None
            }
            Message::Watermark(_) => {
                // Emit final results when receiving watermark
                if !self.accumulators.is_empty() {
                    let messages = self.emit_all_accumulators();
                    Some(messages)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_operator() {
        use crate::api::planner::{Planner, PlanningContext};
        use datafusion::prelude::SessionContext;
        use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
        use crate::common::{KeyedMessage, BaseMessage, WatermarkMessage, MAX_WATERMARK_VALUE};
        use arrow::array::{StringArray, Int64Array, Float64Array};
        use arrow::record_batch::RecordBatch;
        
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
        ]));
        
        planner.register_source(
            "test_table".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema.clone()
        );
        
        let sql = "SELECT name, COUNT(*) as count, SUM(value) as sum_value, AVG(value) as avg_value, MAX(value) as max_value, MIN(value) as min_value FROM test_table GROUP BY name";
        let logical_graph = planner.sql_to_graph(sql).unwrap();
        
        let mut aggregate_operator = None;
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        for node in &nodes {
            if let OperatorConfig::AggregateConfig(config) = &node.operator_config {
                aggregate_operator = Some(AggregateOperator::new(OperatorConfig::AggregateConfig(config.clone())));
                break;
            }
        }
        
        let mut operator = aggregate_operator.expect("Should have found an aggregate operator");
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        let test_data = vec![
            // Multiple records with name "alice" - should count as 3
            ("alice", vec![10i64, 20i64, 30i64]),
            // Multiple records with name "bob" - should count as 2  
            ("bob", vec![40i64, 50i64]),
            // Single record with name "charlie" - should count as 1
            ("charlie", vec![60i64]),
        ];
        
        for (name, values) in test_data {
            for value in values {
                // Create RecordBatch with single row
                let name_array = StringArray::from(vec![name]);
                let value_array = Int64Array::from(vec![value]);
                
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(name_array), Arc::new(value_array)]
                ).unwrap();
                
                // Create a key based on the name (this is our GROUP BY key)
                let key_batch = RecordBatch::try_new(
                    Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
                    ])),
                    vec![Arc::new(StringArray::from(vec![name]))]
                ).unwrap();
                let key = Key::new(key_batch).unwrap();
                
                // Create keyed message
                let keyed_message = KeyedMessage::new(
                    BaseMessage::new(None, batch, Some(0)), // upstream_vertex_id = None, timestamp = 0
                    key
                );
                
                // Process the message
                let _result = rt.block_on(operator.process_message(Message::Keyed(keyed_message)));
                // Intermediate results may or may not be produced depending on implementation
            }
        }
        
        // Send max watermark to finalize aggregation
        let watermark = WatermarkMessage::new("test_operator".to_string(), MAX_WATERMARK_VALUE, None);
        let final_result = rt.block_on(operator.process_message(Message::Watermark(watermark)));
        
        // Verify results
        if let Some(messages) = final_result {
            assert!(!messages.is_empty(), "Should produce final aggregate results");
            
            // Get expected schema from AggregateExec
            let expected_schema = operator.aggregate_exec.schema();
            
            // Verify schema matches for all produced messages
            for message in &messages {
                if let Message::Keyed(keyed_msg) = message {
                    let actual_schema = keyed_msg.base.record_batch.schema();
                    assert_eq!(
                        expected_schema.as_ref(), 
                        actual_schema.as_ref(),
                        "Output schema should match AggregateExec schema.\nExpected: {:?}\nActual: {:?}", 
                        expected_schema.fields(),
                        actual_schema.fields()
                    );
                }
            }
            
            // Check that we have results for each group
            let mut alice_results = (0i64, 0i64, 0.0f64, 0i64, 0i64); // (count, sum, avg, max, min)
            let mut bob_results = (0i64, 0i64, 0.0f64, 0i64, 0i64);
            let mut charlie_results = (0i64, 0i64, 0.0f64, 0i64, 0i64);
            
            for message in messages {
                if let Message::Keyed(keyed_msg) = message {
                    let batch = &keyed_msg.base.record_batch;
                    
                    // Verify we have the expected number of columns
                    assert_eq!(batch.num_columns(), 6, "Should have 6 columns: name, count, sum_value, avg_value, max_value, min_value");
                    
                    // Extract all columns from the batch
                    let name_column = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let count_column = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    let sum_column = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                    let avg_column = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
                    let max_column = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
                    let min_column = batch.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
                    
                    for i in 0..batch.num_rows() {
                        let name = name_column.value(i);
                        let count = count_column.value(i);
                        let sum_value = sum_column.value(i);
                        let avg_value = avg_column.value(i);
                        let max_value = max_column.value(i);
                        let min_value = min_column.value(i);
                        
                        match name {
                            "alice" => alice_results = (count, sum_value, avg_value, max_value, min_value),
                            "bob" => bob_results = (count, sum_value, avg_value, max_value, min_value),
                            "charlie" => charlie_results = (count, sum_value, avg_value, max_value, min_value),
                            other => panic!("Unexpected name in results: {}", other),
                        }
                    }
                }
            }
            
            // Verify expected results for Alice (values: 10, 20, 30)
            assert_eq!(alice_results.0, 3, "Alice should have count of 3");
            assert_eq!(alice_results.1, 60, "Alice should have sum of 60 (10+20+30)");
            assert!((alice_results.2 - 20.0).abs() < 1e-10, "Alice should have avg of 20.0, got {}", alice_results.2);
            assert_eq!(alice_results.3, 30, "Alice should have max of 30");
            assert_eq!(alice_results.4, 10, "Alice should have min of 10");
            
            // Verify expected results for Bob (values: 40, 50)
            assert_eq!(bob_results.0, 2, "Bob should have count of 2");
            assert_eq!(bob_results.1, 90, "Bob should have sum of 90 (40+50)");
            assert!((bob_results.2 - 45.0).abs() < 1e-10, "Bob should have avg of 45.0, got {}", bob_results.2);
            assert_eq!(bob_results.3, 50, "Bob should have max of 50");
            assert_eq!(bob_results.4, 40, "Bob should have min of 40");
            
            // Verify expected results for Charlie (values: 60)
            assert_eq!(charlie_results.0, 1, "Charlie should have count of 1");
            assert_eq!(charlie_results.1, 60, "Charlie should have sum of 60");
            assert!((charlie_results.2 - 60.0).abs() < 1e-10, "Charlie should have avg of 60.0, got {}", charlie_results.2);
            assert_eq!(charlie_results.3, 60, "Charlie should have max of 60");
            assert_eq!(charlie_results.4, 60, "Charlie should have min of 60");
        } else {
            panic!("Expected final aggregate results from watermark message");
        }
    }
}
