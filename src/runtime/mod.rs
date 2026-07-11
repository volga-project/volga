pub mod runtime_context;
pub mod execution_graph;
pub mod operators;
pub mod stream_task;
pub mod stream_task_actor;
pub mod worker;
pub mod worker_config_utils;
pub mod worker_server;
pub mod master;
pub mod functions;
pub mod metrics;
pub mod observability;
pub mod state;
pub mod utils;
pub mod watermark;
pub mod health;

use std::sync::Arc;

/// Operator/task identity within a worker.
///
/// vertex == task
pub type VertexId = Arc<str>;
pub type TaskId = VertexId;

pub mod partition;
pub mod collector;
#[cfg(test)]
pub mod tests;