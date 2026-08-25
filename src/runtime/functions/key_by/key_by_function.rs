use std::sync::Arc;
use async_trait::async_trait;
use crate::api::Planner;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::OperatorConfig;
use anyhow::Result;
use std::fmt;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::key_by::pack::{evaluate_key_arrays, pack_by_dest};
use std::any::Any;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::windows::BoundedWindowAggExec;

/// Source of key expressions from DataFusion physical plans.
#[derive(Debug, Clone)]
enum DFKeyExprSource {
    Aggregate(Arc<AggregateExec>),
    Window(Arc<BoundedWindowAggExec>),
}

/// KeyBy from a DataFusion GROUP BY / PARTITION BY plan.
#[derive(Debug, Clone)]
pub struct KeyByFunction {
    key_expr_source: DFKeyExprSource,
}

impl KeyByFunction {
    pub fn new(aggregate_exec: Arc<AggregateExec>) -> Self {
        Self {
            key_expr_source: DFKeyExprSource::Aggregate(aggregate_exec),
        }
    }

    pub fn new_window(window_exec: Arc<BoundedWindowAggExec>) -> Self {
        Self {
            key_expr_source: DFKeyExprSource::Window(window_exec),
        }
    }

    fn group_exprs(&self) -> Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
        match &self.key_expr_source {
            DFKeyExprSource::Aggregate(agg_exec) => agg_exec.group_expr().input_exprs(),
            DFKeyExprSource::Window(window_exec) => {
                window_exec.window_expr()[0].partition_by().to_vec()
            }
        }
    }

    pub fn key_by(&self, message: Message, num_partitions: usize) -> Vec<Message> {
        let record_batch = message.record_batch();
        if record_batch.num_rows() == 0 {
            panic!("Can not key empty batch");
        }
        let group_exprs = self.group_exprs();
        if group_exprs.is_empty() {
            // No GROUP BY / PARTITION BY (DataFusion also drops `PARTITION BY 1`).
            // One global key: whole batch to dest 0.
            let hashes = vec![0u64; record_batch.num_rows()];
            return pack_by_dest(&message, &hashes, num_partitions);
        }
        let group_arrays = evaluate_key_arrays(&group_exprs, record_batch);
        let hashes =
            Key::hash_arrays(&group_arrays, record_batch.num_rows()).expect("key hashes");
        pack_by_dest(&message, &hashes, num_partitions)
    }
}

impl fmt::Display for KeyByFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataFusion")
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
    use std::collections::HashMap;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use crate::api::planner::{Planner, PlanningContext};
    use crate::common::message::TARGET_SUBTASK_EXTRA;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

    async fn create_test_setup() -> (Planner, Message) {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        let schema = Arc::new(Schema::new(vec![
            Field::new("salary", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
        ]));
        planner.register_source(
            "employees".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
            schema.clone(),
        );
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    80000, 50000, 60000, 90000, 85000, 75000,
                ])),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "alice", "charlie", "alice", "bob",
                ])),
                Arc::new(StringArray::from(vec![
                    "eng", "sales", "sales", "eng", "eng", "eng",
                ])),
            ],
        )
        .unwrap();
        let message = Message::new(None, record_batch, None, None);
        (planner, message)
    }

    fn verify_name_dept_groups(messages: &[Message]) {
        let mut key_to_salaries: HashMap<(String, String), Vec<i32>> = HashMap::new();
        for message in messages {
            let batch = message.record_batch();
            let name_col = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let dept_col = batch
                .column_by_name("department")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let salary_col = batch
                .column_by_name("salary")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                key_to_salaries
                    .entry((name_col.value(i).to_string(), dept_col.value(i).to_string()))
                    .or_default()
                    .push(salary_col.value(i));
            }
        }
        assert_eq!(key_to_salaries.len(), 5);
        let mut alice_eng = key_to_salaries
            .get(&("alice".to_string(), "eng".to_string()))
            .unwrap()
            .clone();
        alice_eng.sort();
        assert_eq!(alice_eng, vec![80000, 85000]);
    }

    fn key_by_from_graph(
        logical_graph: &crate::api::logical_graph::LogicalGraph,
    ) -> KeyByFunction {
        for node in logical_graph.get_nodes() {
            if let crate::runtime::operators::operator::OperatorConfig::KeyByConfig(key_by) =
                &node.operator_config
            {
                return key_by.clone();
            }
        }
        panic!("Should have found a KeyBy");
    }

    #[tokio::test]
    async fn test_datafusion_key_by_aggregate_source() {
        let (mut planner, message) = create_test_setup().await;
        let sql =
            "SELECT name, department, COUNT(*) as count FROM employees GROUP BY name, department";
        let logical_graph = planner.sql_to_graph(sql).unwrap();
        let key_by = key_by_from_graph(&logical_graph);
        let packed = key_by.key_by(message.clone(), 1);
        assert_eq!(packed.len(), 1);
        verify_name_dept_groups(&packed);

        let packed_p2 = key_by.key_by(message, 2);
        assert!(packed_p2.len() >= 1 && packed_p2.len() <= 2);
        assert_eq!(
            packed_p2.iter().map(|m| m.record_batch().num_rows()).sum::<usize>(),
            6
        );
        for m in &packed_p2 {
            let dest: usize = m
                .get_extras()
                .unwrap()
                .get(TARGET_SUBTASK_EXTRA)
                .unwrap()
                .parse()
                .unwrap();
            assert!(dest < 2);
        }
        verify_name_dept_groups(&packed_p2);
    }

    #[tokio::test]
    async fn test_datafusion_key_by_empty_group_packs_to_dest_0() {
        let (mut planner, message) = create_test_setup().await;
        let logical_graph = planner
            .sql_to_graph("SELECT COUNT(*) as count FROM employees")
            .unwrap();
        let key_by = key_by_from_graph(&logical_graph);
        let packed = key_by.key_by(message, 4);
        assert_eq!(packed.len(), 1);
        assert_eq!(packed[0].record_batch().num_rows(), 6);
        let extras = packed[0].get_extras().unwrap();
        assert_eq!(extras.get(TARGET_SUBTASK_EXTRA).unwrap(), "0");
    }

    #[tokio::test]
    async fn test_datafusion_key_by_window_source() {
        let (mut planner, message) = create_test_setup().await;
        let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY name, department ORDER BY salary) as rn FROM employees";
        let window_exec = extract_datafusion_window_exec(sql, &mut planner).await;
        let key_by = KeyByFunction::new_window(window_exec);
        let packed = key_by.key_by(message, 1);
        assert_eq!(packed.len(), 1);
        verify_name_dept_groups(&packed);
    }

    #[tokio::test]
    async fn test_datafusion_key_by_window_no_partition_packs_to_dest_0() {
        let (mut planner, message) = create_test_setup().await;
        let sql = "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM employees";
        let window_exec = extract_datafusion_window_exec(sql, &mut planner).await;
        let key_by = KeyByFunction::new_window(window_exec);
        let packed = key_by.key_by(message, 3);
        assert_eq!(packed.len(), 1);
        let extras = packed[0].get_extras().unwrap();
        assert_eq!(extras.get(TARGET_SUBTASK_EXTRA).unwrap(), "0");
    }
}

// TODO move to test utils
pub async fn extract_datafusion_window_exec(sql: &str, planner: &mut Planner) -> Arc<BoundedWindowAggExec> {
    let logical_graph = planner.sql_to_graph(sql).unwrap();
    let nodes: Vec<_> = logical_graph.get_nodes().collect();

    for node in &nodes {
        if let OperatorConfig::WindowConfig(config) = &node.operator_config {
            return config.window_exec.clone();
        }
    }
    panic!("Should have found a window operator");
}
