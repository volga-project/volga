use std::collections::HashMap;
use serde_json::Value;
use tokio_rayon::rayon::ThreadPool;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    task_id: i64,
    task_index: i32,
    parallelism: i32,
    operator_id: i64,
    operator_name: String,
    job_config: HashMap<String, Value>,
}

impl RuntimeContext {
    pub fn new(
        task_id: i64,
        task_index: i32,
        parallelism: i32,
        operator_id: i64,
        operator_name: String,
        job_config: Option<HashMap<String, Value>>,
    ) -> Self {
        Self {
            task_id,
            task_index,
            parallelism,
            operator_id,
            operator_name,
            job_config: job_config.unwrap_or_default(),
        }
    }

    pub fn task_id(&self) -> i64 { self.task_id }
    pub fn task_index(&self) -> i32 { self.task_index }
    pub fn parallelism(&self) -> i32 { self.parallelism }
    pub fn operator_id(&self) -> i64 { self.operator_id }
    pub fn operator_name(&self) -> &str { &self.operator_name }
    pub fn job_config(&self) -> &HashMap<String, Value> { &self.job_config }
}
