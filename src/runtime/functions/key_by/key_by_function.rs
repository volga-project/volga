use std::sync::Arc;
use async_trait::async_trait;
use crate::api::Planner;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::OperatorConfig;
use anyhow::Result;
use std::fmt;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use crate::runtime::functions::key_by::pack::{
    evaluate_key_arrays, key_fields_for_exprs, pack_by_dest,
};
use std::any::Any;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::windows::BoundedWindowAggExec;

pub trait KeyByFunctionTrait: Send + Sync + fmt::Debug {
    fn key_by(&self, batch: Message, num_partitions: usize) -> Vec<Message>;
}

/// Generic key-by function that can be used for any key-by function
#[derive(Debug, Clone)]
pub struct CustomKeyByFunction {
    function: Arc<dyn KeyByFunctionTrait>,
    runtime_context: Option<RuntimeContext>,
}

impl CustomKeyByFunction {
    pub fn new<F>(function: F) -> Self
    where
        F: KeyByFunctionTrait + 'static,
    {
        Self {
            function: Arc::new(function),
            runtime_context: None,
        }
    }
}

impl KeyByFunctionTrait for CustomKeyByFunction {
    fn key_by(&self, batch: Message, num_partitions: usize) -> Vec<Message> {
        self.function.key_by(batch, num_partitions)
    }
}

/// Hash named payload columns and pack rows by downstream subtask.
#[derive(Debug, Clone)]
pub struct ColumnKeyByFunction {
    key_columns: Vec<String>,
    runtime_context: Option<RuntimeContext>,
}

impl ColumnKeyByFunction {
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            runtime_context: None,
        }
    }
}

impl KeyByFunctionTrait for ColumnKeyByFunction {
    fn key_by(&self, message: Message, num_partitions: usize) -> Vec<Message> {
        let batch = message.record_batch();
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        let schema = batch.schema();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.key_columns.len());
        for name in &self.key_columns {
            let (idx, _) = schema
                .column_with_name(name)
                .unwrap_or_else(|| panic!("Key column '{}' not found", name));
            arrays.push(batch.column(idx).clone());
        }
        let hashes = Key::hash_arrays(&arrays, batch.num_rows()).expect("key hashes");
        pack_by_dest(&message, &hashes, &self.key_columns, num_partitions)
    }
}

/// Source of key expressions from DataFusion physical plans
#[derive(Debug, Clone)]
pub enum DFKeyExprSource {
    Aggregate(Arc<AggregateExec>),
    Window(Arc<BoundedWindowAggExec>),
}

/// DataFusion-based key-by function using physical plan expressions
#[derive(Debug, Clone)]
pub struct DataFusionKeyFunction {
    key_expr_source: DFKeyExprSource,
    runtime_context: Option<RuntimeContext>,
}

impl DataFusionKeyFunction {
    pub fn new(aggregate_exec: Arc<AggregateExec>) -> Self {
        Self {
            key_expr_source: DFKeyExprSource::Aggregate(aggregate_exec),
            runtime_context: None,
        }
    }

    pub fn new_window(window_exec: Arc<BoundedWindowAggExec>) -> Self {
        Self {
            key_expr_source: DFKeyExprSource::Window(window_exec),
            runtime_context: None,
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

    fn key_field_names(&self, arrays: &[ArrayRef], batch_schema: &Schema) -> Vec<String> {
        let exprs = self.group_exprs();
        key_fields_for_exprs(&exprs, arrays, batch_schema)
            .into_iter()
            .map(|f| f.name().clone())
            .collect()
    }
}

impl KeyByFunctionTrait for DataFusionKeyFunction {
    fn key_by(&self, message: Message, num_partitions: usize) -> Vec<Message> {
        let record_batch = message.record_batch();
        if record_batch.num_rows() == 0 {
            panic!("Can not key empty batch");
        }
        let group_exprs = self.group_exprs();
        if group_exprs.is_empty() {
            panic!("No group by expressions");
        }
        let group_arrays = evaluate_key_arrays(&group_exprs, record_batch);
        let hashes =
            Key::hash_arrays(&group_arrays, record_batch.num_rows()).expect("key hashes");
        let names = self.key_field_names(&group_arrays, record_batch.schema().as_ref());
        pack_by_dest(&message, &hashes, &names, num_partitions)
    }
}

#[derive(Debug, Clone)]
pub enum KeyByFunction {
    Custom(CustomKeyByFunction),
    Columns(ColumnKeyByFunction),
    DataFusion(DataFusionKeyFunction),
}

impl fmt::Display for KeyByFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyByFunction::Custom(_) => write!(f, "Custom"),
            KeyByFunction::Columns(_) => write!(f, "Columns"),
            KeyByFunction::DataFusion(_) => write!(f, "DataFusion"),
        }
    }
}

impl KeyByFunctionTrait for KeyByFunction {
    fn key_by(&self, message: Message, num_partitions: usize) -> Vec<Message> {
        match self {
            KeyByFunction::Custom(function) => function.key_by(message, num_partitions),
            KeyByFunction::Columns(function) => function.key_by(message, num_partitions),
            KeyByFunction::DataFusion(function) => function.key_by(message, num_partitions),
        }
    }
}

impl KeyByFunction {
    pub fn new_custom<F>(function: F) -> Self
    where
        F: KeyByFunctionTrait + 'static,
    {
        Self::Custom(CustomKeyByFunction::new(function))
    }

    pub fn new_columns(key_columns: Vec<String>) -> Self {
        Self::Columns(ColumnKeyByFunction::new(key_columns))
    }

    pub fn new_datafusion_key_by(aggregate_exec: Arc<AggregateExec>) -> Self {
        Self::DataFusion(DataFusionKeyFunction::new(aggregate_exec))
    }
}

#[async_trait]
impl FunctionTrait for KeyByFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        match self {
            KeyByFunction::Custom(function) => function.runtime_context = Some(_context.clone()),
            KeyByFunction::Columns(function) => function.runtime_context = Some(_context.clone()),
            KeyByFunction::DataFusion(function) => function.runtime_context = Some(_context.clone()),
        }
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
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::functions::key_by::pack::split_by_key_column_names;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

    fn groups_for_id(messages: &[Message]) -> HashMap<i32, Vec<String>> {
        let mut id_to_values: HashMap<i32, Vec<String>> = HashMap::new();
        for message in messages {
            for (key, batch) in split_by_key_column_names(message.record_batch(), &["id".to_string()])
            {
                let id_col = key
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let id = id_col.value(0);
                let value_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values: Vec<String> = (0..batch.num_rows())
                    .map(|i| value_col.value(i).to_string())
                    .collect();
                id_to_values.entry(id).or_default().extend(values);
            }
        }
        id_to_values
    }

    #[test]
    fn test_column_key_by_packs_by_dest() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 1])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
            ],
        )
        .unwrap();
        let message = Message::new(None, record_batch, None, None);
        let key_by = ColumnKeyByFunction::new(vec!["id".to_string()]);

        let packed = key_by.key_by(message.clone(), 1);
        assert_eq!(packed.len(), 1);
        assert_eq!(packed[0].record_batch().num_rows(), 6);
        let extras = packed[0].get_extras().unwrap();
        assert_eq!(extras.get(crate::common::message::TARGET_SUBTASK_EXTRA).unwrap(), "0");
        assert_eq!(extras.get(crate::common::message::KEY_FIELDS_EXTRA).unwrap(), "id");
        let groups = groups_for_id(&packed);
        assert_eq!(groups.len(), 3);
        assert_eq!(
            groups.get(&1).unwrap(),
            &vec!["a".to_string(), "c".to_string(), "f".to_string()]
        );
        assert_eq!(
            groups.get(&2).unwrap(),
            &vec!["b".to_string(), "e".to_string()]
        );
        assert_eq!(groups.get(&3).unwrap(), &vec!["d".to_string()]);

        let packed_p2 = key_by.key_by(message, 2);
        assert!(packed_p2.len() <= 2);
        assert_eq!(
            packed_p2.iter().map(|m| m.num_records()).sum::<usize>(),
            6
        );
    }

    async fn create_test_setup() -> (Planner, Arc<Schema>, Message) {
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
            schema.clone(),
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
        (planner, schema, message)
    }

    fn verify_name_dept_groups(messages: &[Message]) {
        let mut key_to_salaries: HashMap<(String, String), Vec<i32>> = HashMap::new();
        for message in messages {
            for (key, batch) in
                split_by_key_column_names(message.record_batch(), &["name".to_string(), "department".to_string()])
            {
                let kb = key.record_batch();
                let name = kb
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string();
                let dept = kb
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string();
                let salary_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let salaries: Vec<i32> = (0..batch.num_rows()).map(|i| salary_col.value(i)).collect();
                key_to_salaries.entry((name, dept)).or_default().extend(salaries);
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

    fn datafusion_from_graph(
        logical_graph: &crate::api::logical_graph::LogicalGraph,
    ) -> DataFusionKeyFunction {
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        for node in &nodes {
            if let crate::runtime::operators::operator::OperatorConfig::KeyByConfig(key_by_function) =
                &node.operator_config
            {
                if let KeyByFunction::DataFusion(key_by) = key_by_function {
                    return key_by.clone();
                }
            }
        }
        panic!("Should have found a DataFusion key_by");
    }

    #[tokio::test]
    async fn test_datafusion_key_by_aggregate_source() {
        let (mut planner, _schema, message) = create_test_setup().await;
        let sql =
            "SELECT name, department, COUNT(*) as count FROM employees GROUP BY name, department";
        let logical_graph = planner.sql_to_graph(sql).unwrap();
        let key_by = datafusion_from_graph(&logical_graph);
        let packed = key_by.key_by(message, 1);
        assert_eq!(packed.len(), 1);
        verify_name_dept_groups(&packed);
    }

    #[tokio::test]
    async fn test_datafusion_key_by_window_source() {
        let (mut planner, _, message) = create_test_setup().await;
        let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY name, department ORDER BY salary) as rn FROM employees";
        let window_exec = extract_datafusion_window_exec(sql, &mut planner).await;
        let key_by = DataFusionKeyFunction::new_window(window_exec);
        let packed = key_by.key_by(message, 1);
        assert_eq!(packed.len(), 1);
        verify_name_dept_groups(&packed);
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
