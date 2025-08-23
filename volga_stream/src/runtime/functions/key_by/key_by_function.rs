use std::sync::Arc;
use async_trait::async_trait;
use crate::common::message::{Message, KeyedMessage, BaseMessage};
use crate::common::Key;
use anyhow::Result;
use std::fmt;
use arrow::compute;
use arrow::compute::kernels::partition::partition;
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use std::collections::HashMap;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::common::hash_utils::create_hashes;
use ahash::RandomState;

pub trait KeyByFunctionTrait: Send + Sync + fmt::Debug {
    fn key_by(&self, batch: Message) -> Vec<KeyedMessage>;
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
    fn key_by(&self, batch: Message) -> Vec<KeyedMessage> {
        self.function.key_by(batch)
    }
}

/// Arrow-based key-by function using RecordBatch for stable key representation
#[derive(Debug, Clone)]
pub struct ArrowKeyByFunction {
    key_columns: Vec<String>,
    runtime_context: Option<RuntimeContext>,
}

impl ArrowKeyByFunction {
    pub fn new(key_columns: Vec<String>) -> Self {
        Self { key_columns, runtime_context: None }
    }

    /// Fast lexicographic sort implementation using arrow-row
    fn fast_lexsort_indices(&self, arrays: &[ArrayRef]) -> UInt32Array {
        // Create SortField objects for each array
        let fields: Vec<SortField> = arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect();
        
        // Create a RowConverter for the fields
        let converter = RowConverter::new(fields).unwrap();
        
        // Convert columns to row format
        let rows = converter.convert_columns(arrays).unwrap();
        
        // Sort the rows
        let mut indices: Vec<_> = rows.iter().enumerate().collect();
        indices.sort_by(|(_, a), (_, b)| a.cmp(b));
        
        // Create the UInt32Array of sorted indices
        UInt32Array::from_iter_values(indices.iter().map(|(i, _)| *i as u32))
    }
}

impl KeyByFunctionTrait for ArrowKeyByFunction {
    fn key_by(&self, message: Message) -> Vec<KeyedMessage> {
        let record_batch = message.record_batch();
        let schema = record_batch.schema();
        
        // If batch is empty, return empty result
        if record_batch.num_rows() == 0 {
            return Vec::new();
        }
        
        // Step 1: Identify the key columns
        let mut key_column_indices = Vec::with_capacity(self.key_columns.len());
        let mut key_fields = Vec::with_capacity(self.key_columns.len());
        let mut key_arrays = Vec::with_capacity(self.key_columns.len());
        
        for key_column in &self.key_columns {
            if let Some((idx, field)) = schema.column_with_name(key_column) {
                key_column_indices.push(idx);
                key_fields.push(field.clone());
                key_arrays.push(record_batch.column(idx).clone());
            } else {
                panic!("Key column '{}' not found", key_column);
            }
        }
        
        // Step 2: Sort the data using arrow-row's fast sorting
        let sorted_indices = self.fast_lexsort_indices(&key_arrays);
        
        // Step 3: Create sorted arrays for all columns
        let mut sorted_columns = Vec::with_capacity(record_batch.num_columns());
        for i in 0..record_batch.num_columns() {
            let sorted_array = compute::take(record_batch.column(i).as_ref(), &sorted_indices, None).unwrap();
            sorted_columns.push(sorted_array);
        }
        
        // Create a new sorted batch
        let sorted_batch = RecordBatch::try_new(record_batch.schema(), sorted_columns).unwrap();
        
        // Step 4: Extract sorted key columns
        let sorted_key_arrays: Vec<ArrayRef> = key_column_indices
            .iter()
            .map(|&idx| sorted_batch.column(idx).clone())
            .collect();
        
        // Step 5: Use Arrow's partition function to find ranges with equal keys
        let partitions = partition(&sorted_key_arrays).unwrap();
        let ranges = partitions.ranges();
        
        // Step 6: Create keyed messages for each partition
        let mut keyed_messages = Vec::with_capacity(ranges.len());
        
        for range in ranges {
            // Extract this range from the sorted batch
            let group_batch = sorted_batch.slice(range.start, range.end - range.start);
            
            // Create a key from the first row of the key columns
            let key_arrays_for_first_row: Vec<ArrayRef> = key_column_indices
                .iter()
                .map(|&idx| {
                    let array = group_batch.column(idx);
                    let indices = UInt32Array::from(vec![0]);  // Get just the first row
                    compute::take(array.as_ref(), &indices, None).unwrap()
                })
                .collect();
            
            // Create key RecordBatch with a single row
            let key_schema = Arc::new(Schema::new(key_fields.clone()));
            let key_batch = RecordBatch::try_new(key_schema, key_arrays_for_first_row).unwrap();
            
            // Create Key object
            let key = Key::new(key_batch).unwrap();
            
            // Create a KeyedMessage with this partition data
            let keyed_message = KeyedMessage::new(
                BaseMessage::new(message.upstream_vertex_id(), group_batch, message.ingest_timestamp()),
                key,
            );
            
            keyed_messages.push(keyed_message);
        }
        
        keyed_messages
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
            runtime_context: None 
        }
    }

    pub fn new_window(window_exec: Arc<BoundedWindowAggExec>) -> Self {
        Self {
            key_expr_source: DFKeyExprSource::Window(window_exec),
            runtime_context: None
        }
    }
}

impl KeyByFunctionTrait for DataFusionKeyFunction {

    // TODO use evaluate_group_by from DataFusion
    fn key_by(&self, message: Message) -> Vec<KeyedMessage> {
        let record_batch = message.record_batch();
        // If batch is empty, return empty result
        if record_batch.num_rows() == 0 {
            panic!("Can not key empty batch")
        }

        // Step 1: Get key expressions based on the source type
        let group_exprs = match &self.key_expr_source {
            DFKeyExprSource::Aggregate(agg_exec) => agg_exec.group_expr().input_exprs(),
            DFKeyExprSource::Window(window_exec) => window_exec.window_expr()[0].partition_by().to_vec(),
        };
        
        if group_exprs.is_empty() {
            panic!("No group by expressions")
        }
        
        // Evaluate the input expressions directly against the runtime batch
        let group_arrays: Vec<ArrayRef> = group_exprs
            .iter()
            .map(|expr| {
                let value = expr.evaluate(record_batch)
                    .expect("Failed to evaluate group by expression");
                value.into_array(record_batch.num_rows())
                    .expect("Failed to convert to array")
            })
            .collect();
        
        // Step 2: Create hashes for each row based on group values
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut hashes_buffer = vec![0u64; record_batch.num_rows()];
        
        if create_hashes(&group_arrays, &random_state, &mut hashes_buffer).is_err() {
            panic!("Unable to compute hashes for key by")
        }
        
        // Step 3: Group rows by hash values
        let mut hash_to_indices: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, &hash) in hashes_buffer.iter().enumerate() {
            hash_to_indices.entry(hash).or_insert_with(Vec::new).push(i);
        }

        // Step 4: Create keyed messages for each group
        let mut keyed_messages = Vec::new();
        
        for (_hash, indices) in hash_to_indices {
            // Create RecordBatch for this group using take
            let indices_array = UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
            
            let group_columns: Vec<_> = record_batch
                .columns()
                .iter()
                .map(|col| compute::take(col.as_ref(), &indices_array, None).expect("should be able to take values for group"))
                .collect();
                
            let group_batch = RecordBatch::try_new(record_batch.schema(), group_columns).expect("should be able to create batch for group");
            
            let schema = match &self.key_expr_source {
                DFKeyExprSource::Aggregate(agg_exec) => agg_exec.schema(),
                DFKeyExprSource::Window(window_exec) => window_exec.schema(),
            };
            
            // Extract key values from the group_arrays using the first index of this group
            let group_first_index = UInt32Array::from(vec![indices[0] as u32]);
            
            let (key_arrays, key_fields): (Vec<ArrayRef>, Vec<_>) = group_arrays
                .iter()
                .enumerate()
                .map(|(i, array)| {
                    // Create key array - take the first row of this specific group from the global arrays
                    let key_array = compute::take(array.as_ref(), &group_first_index, None).unwrap();
                    
                    // Create key field - get name from AggregateExec schema (group by fields come first)
                    if i >= schema.fields().len() {
                        panic!(
                            "AggregateExec schema has {} fields but trying to access group field at index {}",
                            schema.fields().len(), i
                        );
                    }
                    
                    let field_name = schema.fields()[i].name().clone();
                    let key_field = arrow::datatypes::Field::new(
                        field_name,
                        array.data_type().clone(),
                        true
                    );
                    
                    (key_array, key_field)
                })
                .unzip();
                
            let key_schema = Arc::new(Schema::new(key_fields));
            let key_batch = RecordBatch::try_new(key_schema, key_arrays).expect("should be able to creat key batch");
            
            // TODO this will re-compute hash - can we somehow change Key logic to use existing hashes?
            let key = Key::new(key_batch).expect("should be able to create key");
            
            let keyed_message = KeyedMessage::new(
                BaseMessage::new(message.upstream_vertex_id(), group_batch, message.ingest_timestamp()),
                key,
            );
            
            keyed_messages.push(keyed_message);
        }
        
        keyed_messages
    }
}

#[derive(Debug, Clone)]
pub enum KeyByFunction {
    Custom(CustomKeyByFunction),
    Arrow(ArrowKeyByFunction),
    DataFusion(DataFusionKeyFunction),
}

impl fmt::Display for KeyByFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyByFunction::Custom(_) => write!(f, "Custom"),
            KeyByFunction::Arrow(_) => write!(f, "Arrow"),
            KeyByFunction::DataFusion(_) => write!(f, "DataFusion"),
        }
    }
}

impl KeyByFunctionTrait for KeyByFunction {
    fn key_by(&self, message: Message) -> Vec<KeyedMessage> {
        match self {
            KeyByFunction::Custom(function) => function.key_by(message),
            KeyByFunction::Arrow(function) => function.key_by(message),
            KeyByFunction::DataFusion(function) => function.key_by(message),
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
    
    pub fn new_arrow_key_by(key_columns: Vec<String>) -> Self {
        Self::Arrow(ArrowKeyByFunction::new(key_columns))
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
            KeyByFunction::Arrow(function) => function.runtime_context = Some(_context.clone()),
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
    use arrow::array::{Int32Array, Float64Array, StringArray};
    use arrow::datatypes::{Field, DataType};
    use datafusion::prelude::SessionContext;
    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

    #[test]
    fn test_arrow_key_by_single_column() {
        // Create a test batch with a single key column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 1]);
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(value_array)]
        ).unwrap();
        
        let message = Message::new(None, record_batch, None);
        
        // Create a key-by function with 'id' as the key column
        let key_by_function = ArrowKeyByFunction::new(vec!["id".to_string()]);
        
        // Execute key-by
        let keyed_messages = key_by_function.key_by(message);
        
        // We should have 3 distinct keys (1, 2, 3)
        assert_eq!(keyed_messages.len(), 3);
        
        // Verify the contents of each keyed message
        let mut id_to_values: HashMap<i32, Vec<String>> = HashMap::new();
        
        for keyed_message in &keyed_messages {
            let batch = &keyed_message.base.record_batch;
            let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let value_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            
            // All rows in a batch should have the same id
            let id = id_col.value(0);
            assert!(id_col.iter().all(|val| val == Some(id)), 
                "Not all IDs in batch are equal: {:?}", id_col);
            
            // Collect values for this id
            let values: Vec<String> = (0..batch.num_rows())
                .map(|i| value_col.value(i).to_string())
                .collect();
            
            id_to_values.insert(id, values);
        }
        
        // Verify exact contents of each group
        assert_eq!(id_to_values.len(), 3);
        
        // ID 1 should have values "a", "c", "f" in exact order
        let values_for_id_1 = id_to_values.get(&1).unwrap();
        assert_eq!(values_for_id_1.len(), 3);
        assert_eq!(*values_for_id_1, vec!["a".to_string(), "c".to_string(), "f".to_string()]);
        
        // ID 2 should have values "b", "e" in exact order
        let values_for_id_2 = id_to_values.get(&2).unwrap();
        assert_eq!(values_for_id_2.len(), 2);
        assert_eq!(*values_for_id_2, vec!["b".to_string(), "e".to_string()]);
        
        // ID 3 should have only value "d"
        let values_for_id_3 = id_to_values.get(&3).unwrap();
        assert_eq!(values_for_id_3.len(), 1);
        assert_eq!(values_for_id_3[0], "d");
    }

    #[test]
    fn test_arrow_key_by_multiple_columns() {
        // Create a test batch with multiple key columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("subcategory", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let category_array = Int32Array::from(vec![1, 1, 1, 2, 2, 3]);
        let subcategory_array = Int32Array::from(vec![1, 2, 1, 1, 1, 1]);
        let value_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(category_array), 
                Arc::new(subcategory_array), 
                Arc::new(value_array)
            ]
        ).unwrap();
        
        let message = Message::new(None, record_batch, None);
        
        // Create a key-by function with both columns as keys
        let key_by_function = ArrowKeyByFunction::new(
            vec!["category".to_string(), "subcategory".to_string()]
        );
        
        // Execute key-by
        let keyed_messages = key_by_function.key_by(message);
        
        // We should have 4 distinct keys: (1,1), (1,2), (2,1), (3,1)
        assert_eq!(keyed_messages.len(), 4);
        
        // Map to store key -> values
        let mut key_to_values: HashMap<(i32, i32), Vec<f64>> = HashMap::new();
        
        for keyed_message in &keyed_messages {
            let batch = &keyed_message.base.record_batch;
            let cat_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let subcat_col = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            let value_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            
            // All rows should have the same category and subcategory
            let cat = cat_col.value(0);
            let subcat = subcat_col.value(0);
            
            // Collect all values for this key
            let values: Vec<f64> = (0..batch.num_rows())
                .map(|i| value_col.value(i))
                .collect();
            
            key_to_values.insert((cat, subcat), values);
        }
        
        // Verify specific contents for each key group
        assert_eq!(key_to_values.len(), 4);
        
        // Key (1,1) should have values [10.0, 30.0] in exact order
        let values_1_1 = key_to_values.get(&(1, 1)).unwrap();
        assert_eq!(values_1_1.len(), 2);
        assert_eq!(*values_1_1, vec![10.0, 30.0]);
        
        // Key (1,2) should have only value 20.0
        let values_1_2 = key_to_values.get(&(1, 2)).unwrap();
        assert_eq!(values_1_2.len(), 1);
        assert_eq!(*values_1_2, vec![20.0]);
        
        // Key (2,1) should have values [40.0, 50.0] in exact order
        let values_2_1 = key_to_values.get(&(2, 1)).unwrap();
        assert_eq!(values_2_1.len(), 2);
        assert_eq!(*values_2_1, vec![40.0, 50.0]);
        
        // Key (3,1) should have only value 60.0
        let values_3_1 = key_to_values.get(&(3, 1)).unwrap();
        assert_eq!(values_3_1.len(), 1);
        assert_eq!(*values_3_1, vec![60.0]);
    }

    // Helper function to create test setup
    async fn create_test_setup() -> (Planner, Arc<Schema>, Message) {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        
        // Create schema with employee data
        let schema = Arc::new(Schema::new(vec![
            Field::new("salary", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
        ]));
        
        // Register source table
        planner.register_source(
            "employees".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema.clone()
        );

        // Create test data - same employee can be in different departments
        let name_array = StringArray::from(vec![
            "alice", "bob", "alice", "charlie", "alice", "bob"
        ]);
        let dept_array = StringArray::from(vec![
            "eng", "sales", "sales", "eng", "eng", "eng"  
        ]);
        let salary_array = Int32Array::from(vec![
            80000, 50000, 60000, 90000, 85000, 75000
        ]);
        
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(salary_array),
                Arc::new(name_array), 
                Arc::new(dept_array),
            ]
        ).unwrap();
        
        let message = Message::new(None, record_batch, None);
        
        (planner, schema, message)
    }

    // Helper function to verify keyed message results
    fn verify_keyed_messages(keyed_messages: Vec<KeyedMessage>) {
        // We should have 5 distinct groups
        assert_eq!(keyed_messages.len(), 5);
        
        // Map to store (name, department) -> salaries
        let mut key_to_salaries: HashMap<(String, String), Vec<i32>> = HashMap::new();
        
        for keyed_message in &keyed_messages {
            let batch = &keyed_message.base.record_batch;
            let name_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let dept_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let salary_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            
            // All rows in this batch should have the same name and department
            let name = name_col.value(0).to_string();
            let dept = dept_col.value(0).to_string();
            
            // Verify all rows have the same key values
            for i in 0..batch.num_rows() {
                assert_eq!(name_col.value(i), name);
                assert_eq!(dept_col.value(i), dept);
            }
            
            // Collect all salaries for this key
            let salaries: Vec<i32> = (0..batch.num_rows())
                .map(|i| salary_col.value(i))
                .collect();
            
            key_to_salaries.insert((name, dept), salaries);
        }
        
        // Verify we have exactly 5 groups
        assert_eq!(key_to_salaries.len(), 5);
        
        // Verify specific group contents:
        
        // (alice, eng) should have salaries [80000, 85000]
        let alice_eng = key_to_salaries.get(&("alice".to_string(), "eng".to_string())).unwrap();
        assert_eq!(alice_eng.len(), 2);
        let mut alice_eng_sorted = alice_eng.clone();
        alice_eng_sorted.sort();
        assert_eq!(alice_eng_sorted, vec![80000, 85000]);
        
        // (bob, sales) should have salary [50000]
        let bob_sales = key_to_salaries.get(&("bob".to_string(), "sales".to_string())).unwrap();
        assert_eq!(bob_sales.len(), 1);
        assert_eq!(*bob_sales, vec![50000]);
        
        // (alice, sales) should have salary [60000]
        let alice_sales = key_to_salaries.get(&("alice".to_string(), "sales".to_string())).unwrap();
        assert_eq!(alice_sales.len(), 1);
        assert_eq!(*alice_sales, vec![60000]);
        
        // (charlie, eng) should have salary [90000]
        let charlie_eng = key_to_salaries.get(&("charlie".to_string(), "eng".to_string())).unwrap();
        assert_eq!(charlie_eng.len(), 1);
        assert_eq!(*charlie_eng, vec![90000]);
        
        // (bob, eng) should have salary [75000]
        let bob_eng = key_to_salaries.get(&("bob".to_string(), "eng".to_string())).unwrap();
        assert_eq!(bob_eng.len(), 1);
        assert_eq!(*bob_eng, vec![75000]);
    }

    #[tokio::test]
    async fn test_datafusion_key_by_aggregate_source() {
        let (mut planner, _schema, message) = create_test_setup().await;
        
        // Create SQL query with multiple GROUP BY columns
        let sql = "SELECT name, department, COUNT(*) as count FROM employees GROUP BY name, department";
        let logical_graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Extract AggregateExec from the graph
        let mut aggregate_exec: Option<Arc<AggregateExec>> = None;
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        for node in &nodes {
            if let crate::runtime::operators::operator::OperatorConfig::KeyByConfig(key_by_function) = &node.operator_config {
                if let KeyByFunction::DataFusion(key_by) = key_by_function {
                    if let DFKeyExprSource::Aggregate(agg_exec) = &key_by.key_expr_source {
                        aggregate_exec = Some(agg_exec.clone());
                        break;
                    }
                }
            }
        }
        
        let aggregate_exec = aggregate_exec.expect("Should have found an aggregate operator");
        
        // Create DataFusionKeyFunction and test
        let key_by_function = DataFusionKeyFunction::new(aggregate_exec);
        let keyed_messages = key_by_function.key_by(message);
        
        verify_keyed_messages(keyed_messages);
    }

    #[tokio::test]
    async fn test_datafusion_key_by_window_source() {
        let (mut planner, _schema, message) = create_test_setup().await;
        
        // Create SQL query with window function partitioned by multiple columns
        let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY name, department ORDER BY salary) as rn FROM employees";
        let logical_graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Extract WindowAggExec from the graph
        let mut window_exec: Option<Arc<BoundedWindowAggExec>> = None;
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        for node in &nodes {
            if let crate::runtime::operators::operator::OperatorConfig::KeyByConfig(key_by_function) = &node.operator_config {
                if let KeyByFunction::DataFusion(key_by) = key_by_function {
                    if let DFKeyExprSource::Window(win_exec) = &key_by.key_expr_source {
                        window_exec = Some(win_exec.clone());
                        break;
                    }
                }
            }
        }
        
        let window_exec = window_exec.expect("Should have found a window operator");
        
        // Create DataFusionKeyFunction with window source and test
        let key_by_function = DataFusionKeyFunction::new_window(window_exec);
        let keyed_messages = key_by_function.key_by(message);
        
        verify_keyed_messages(keyed_messages);
    }
} 