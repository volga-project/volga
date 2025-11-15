use std::collections::HashMap;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;

// use datafusion::common::Result;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::{Accumulator, ExecutionPlan};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::PhysicalExpr;
use async_trait::async_trait;
use futures::StreamExt;
use anyhow::Result as AnyhowResult;
use crate::runtime::operators::operator::{OperatorTrait, OperatorBase, OperatorType, OperatorConfig, OperatorPollResult, MessageStream};
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::{BaseMessage, Message, MAX_WATERMARK_VALUE};
use std::sync::Arc;
use std::fmt;

#[derive(Debug, Clone)]
pub struct AggregateConfig {
    pub aggregate_exec: AggregateExec,
    pub group_input_exprs: Vec<Arc<dyn PhysicalExpr>>, // from Partial AggregateExec
}

pub struct AggregateOperator {
    base: OperatorBase,
    aggregate_exec: Arc<AggregateExec>,  // DataFusion's AggregateExec - Final
    group_input_exprs: Vec<Arc<dyn PhysicalExpr>>, // evaluated on input batches
    accumulators: HashMap<u64, (BaseMessage, Vec<Box<dyn Accumulator>>)>, // (first message for key, accumulators)
}

impl fmt::Debug for AggregateOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateOperator")
            .field("base", &self.base)
            .field("aggregate_exec", &self.aggregate_exec)
            .field("group_input_exprs", &self.group_input_exprs)
            .field("accumulators", &self.accumulators)
            // .field("input_stream", &"<MessageStream>")
            // .field("pending_messages", &self.pending_messages)
            .finish()
    }
}

impl AggregateOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let aggregate_confg = match config.clone() {
            OperatorConfig::AggregateConfig(aggregate_config) => aggregate_config,
            _ => panic!("Expected AggregateConfig, got {:?}", config),
        };

        Self {
            base: OperatorBase::new(config),
            aggregate_exec: Arc::new(aggregate_confg.aggregate_exec),
            group_input_exprs: aggregate_confg.group_input_exprs,
            accumulators: HashMap::new(),
        }
    }
    
    fn create_accumulators(aggregate_exec: &AggregateExec) -> Vec<Box<dyn Accumulator>> {
        aggregate_exec.aggr_expr()
            .iter()
            .map(|expr| {
                expr.create_accumulator().expect("should be able to create accumulator")
            })
            .collect()
    }
    
    fn emit_all_accumulators(&mut self) -> Option<Message> {
        let mut batches = Vec::new();
        let accumulators: Vec<_> = self.accumulators.drain().collect();
        
        if accumulators.is_empty() {
            return None;
        }

        // Debug: Print vertex_id and emission info
        let vertex_id = if let Some(ctx) = &self.base.runtime_context {
            ctx.vertex_id()
        } else {
            "unknown_vertex"
        };
        
        let mut words_being_emitted = Vec::new();

        // Get the first message to preserve metadata
        let (_, (first_message, _)) = &accumulators[0];
        let upstream_vertex_id = first_message.metadata.upstream_vertex_id.clone();
        let ingest_timestamp = first_message.metadata.ingest_timestamp;
        let extras = first_message.metadata.extras.clone();

        for (_key, (first_message, accumulators)) in accumulators {
            // Build columns in the exact order of the final output schema:
            // 1) group-by output columns
            // 2) aggregate result columns
            let mut columns = Vec::new();

            // Get non-aggregate group-by output columns (which align with input_exprs order) from the first stored message for this key
            let group_arrays: Vec<ArrayRef> = self.group_input_exprs
                .iter()
                .map(|expr| {
                    let v = expr
                        .evaluate(&first_message.record_batch)
                        .expect("should be able to evaluate group expr");
                    match v {
                        ColumnarValue::Array(a) => a.slice(0, 1),
                        ColumnarValue::Scalar(s) => s
                            .to_array_of_size(1)
                            .expect("should convert scalar to array"),
                    }
                })
                .collect();

            // Debug: Extract word for logging
            if let Some(word_array) = group_arrays[0].as_any().downcast_ref::<arrow::array::StringArray>() {
                let word = word_array.value(0);
                words_being_emitted.push(word.to_string());
            }

            let base_len = group_arrays.len();

            // Push base group columns first
            for i in 0..base_len {
                columns.push(group_arrays[i].clone());
            }

            // Append aggregate result columns
            let mut accs = accumulators;
            for accumulator in &mut accs {
                let result = accumulator
                    .evaluate()
                    .expect("should be able to evaluate accumulator");
                let result_array = result
                    .to_array_of_size(1)
                    .expect("should be able to convert to array");
                columns.push(result_array);
            }

            let output_physical_schema = self.aggregate_exec.schema();
            let batch_result = RecordBatch::try_new(output_physical_schema.clone(), columns)
                .expect("should be able to create output batch");

            batches.push(batch_result);
        }
        
        // Concatenate all batches into a single batch
        if batches.is_empty() {
            return None;
        }
        
        let output_physical_schema = self.aggregate_exec.schema();
        let concatenated_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&output_physical_schema, &batches)
                .expect("should be able to concatenate batches")
        };
        
        // Debug: Print emission info
        println!("EMIT: vertex_id={} emitting {} words: {:?}", vertex_id, words_being_emitted.len(), words_being_emitted);
        
        // Check if we've already emitted (this shouldn't happen with proper watermark handling)
        if words_being_emitted.is_empty() {
            println!("WARNING: {} tried to emit but no words found!", vertex_id);
        }
        
        // Return single message with concatenated batch
        Some(Message::new(
            upstream_vertex_id,
            concatenated_batch,
            ingest_timestamp,
            extras
        ))
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

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }
    
    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        // First, return any buffered messages
        if let Some(msg) = self.base.pending_messages.pop() {
            return OperatorPollResult::Ready(msg);
        }
        
        // Then process input stream
        let input_stream = self.base.input.as_mut().expect("input should exist");
            
        match input_stream.next().await {
            Some(message) => {
                match message {
                    Message::Keyed(keyed_message) => {
                        // Inline process_message logic
                        let key = keyed_message.key().clone();
                        let aggregate_exec = self.aggregate_exec.clone(); // Clone before mutable borrow
                        
                        // Get or create accumulators for this key
                        if !self.accumulators.contains_key(&key.hash()) {
                            let accs = Self::create_accumulators(&aggregate_exec);
                            self.accumulators.insert(key.hash(), (keyed_message.base.clone(), accs));
                        }
                        let (_first_message, accumulators) = self.accumulators
                            .get_mut(&key.hash())
                            .expect("Accumulators should exist after insert");
                        
                        // Update accumulators
                        for (i, accumulator) in accumulators.iter_mut().enumerate() {
                            if let Some(aggr_expr) = aggregate_exec.aggr_expr().get(i) {
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
                                
                                if arrays.is_empty() {
                                    panic!("No arrays to update accumulator");
                                }

                                let _ = accumulator.update_batch(&arrays);
                            }
                        }
                        // No immediate emission - return Continue
                        OperatorPollResult::Continue
                    }
                    Message::Watermark(watermark) => {
                        // Inline process_watermark logic
                        // TODO why do we emit only on max watermark?
                        if watermark.watermark_value == MAX_WATERMARK_VALUE {
                            if !self.accumulators.is_empty() {
                                // Emit aggregated result first
                                if let Some(result_msg) = self.emit_all_accumulators() {
                                    // Buffer the watermark to be returned on next poll
                                    self.base.pending_messages.push(Message::Watermark(watermark));
                                    return OperatorPollResult::Ready(result_msg);
                                }
                            }
                        }
                        // Always pass through watermarks (if no result was emitted)
                        OperatorPollResult::Ready(Message::Watermark(watermark))
                    }
                    _ => {
                        panic!("Aggregate operator expects keyed messages or watermarks");
                    }
                }
            }
            None => OperatorPollResult::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_aggregate_operator() {
        use crate::api::planner::{Planner, PlanningContext};
        use datafusion::prelude::SessionContext;
        use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
        use crate::common::{Key, KeyedMessage, BaseMessage, MAX_WATERMARK_VALUE};
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
        
        let test_data = vec![
            // Multiple records with name "alice" - should count as 3
            ("alice", vec![10i64, 20i64, 30i64]),
            // Multiple records with name "bob" - should count as 2  
            ("bob", vec![40i64, 50i64]),
            // Single record with name "charlie" - should count as 1
            ("charlie", vec![60i64]),
        ];
        
        let mut messages_to_process = Vec::new();
        
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
                    BaseMessage::new(None, batch, Some(0), None), // upstream_vertex_id = None, timestamp = 0
                    key
                );
                
                // Collect messages for stream processing
                messages_to_process.push(Message::Keyed(keyed_message));
            }
        }
        
        // Add max watermark to finalize aggregation
        messages_to_process.push(Message::Watermark(crate::common::WatermarkMessage::new(
            "test".to_string(),
            MAX_WATERMARK_VALUE,
            Some(0)
        )));
        
        // Get expected schema from AggregateExec before moving operator
        let expected_schema = operator.aggregate_exec.schema();
        
        // Create input stream from collected messages
        let input_stream = Box::pin(futures::stream::iter(messages_to_process));
        
        // Process through message stream
        operator.set_input(Some(input_stream));
        let mut final_result = Vec::new();
        
        while let result = operator.poll_next().await {
            match result {
                OperatorPollResult::None => {
                    break;
                }
                OperatorPollResult::Continue => {
                    continue;
                }
                OperatorPollResult::Ready(message) => {
                    if !matches!(message, Message::Watermark(_)) {
                        final_result.push(message);
                    }
                }
            }
        }
        
        // Verify results
        assert!(!final_result.is_empty(), "Should produce at least one message");
        assert_eq!(final_result.len(), 1, "Should produce exactly one message with concatenated results");
        
        let message = &final_result[0];
        let batch = message.record_batch();
        let actual_schema = batch.schema();
        assert_eq!(
            expected_schema.as_ref(), 
            actual_schema.as_ref(),
            "Output schema should match AggregateExec schema.\nExpected: {:?}\nActual: {:?}", 
            expected_schema.fields(),
            actual_schema.fields()
        );
        
        // Verify we have the expected number of columns and rows
        assert_eq!(batch.num_columns(), 6, "Should have 6 columns: name, count, sum_value, avg_value, max_value, min_value");
        assert_eq!(batch.num_rows(), 3, "Should have 3 rows (one for each group: alice, bob, charlie)");
        
        // Extract all columns from the batch
        let name_column = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let count_column = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sum_column = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let avg_column = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        let min_column = batch.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Check that we have results for each group
        let mut alice_results = (0i64, 0i64, 0.0f64, 0i64, 0i64); // (count, sum, avg, max, min)
        let mut bob_results = (0i64, 0i64, 0.0f64, 0i64, 0i64);
        let mut charlie_results = (0i64, 0i64, 0.0f64, 0i64, 0i64);
        
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
    }
}

