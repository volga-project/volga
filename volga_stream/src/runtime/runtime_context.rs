use std::collections::HashMap;
use serde_json::Value;
use tokio_rayon::rayon::ThreadPool;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    vertex_id: String,
    task_index: i32,
    parallelism: i32,
    job_config: HashMap<String, Value>,
}

impl RuntimeContext {
    pub fn new(
        vertex_id: String,
        task_index: i32,
        parallelism: i32,
        job_config: Option<HashMap<String, Value>>,
    ) -> Self {
        Self {
            vertex_id,
            task_index,
            parallelism,
            job_config: job_config.unwrap_or_default(),
        }
    }

    pub fn vertex_id(&self) -> &str { &self.vertex_id }
    pub fn task_index(&self) -> i32 { self.task_index }
    pub fn parallelism(&self) -> i32 { self.parallelism }
    pub fn job_config(&self) -> &HashMap<String, Value> { &self.job_config }
}
