pub mod checkpoint;
pub mod consts;
pub mod execution_graph;
pub mod functions;
pub mod health;
pub mod master;
pub mod metrics;
pub mod observability;
pub mod operators;
pub mod runtime_context;
pub mod state;
pub mod stream_task;
pub mod stream_task_actor;
pub mod utils;
pub mod watermark;
pub mod worker;
pub mod worker_config_utils;
pub mod worker_server;

use std::sync::Arc;

/// Operator/task identity within a worker.
///
/// vertex == task
pub type VertexId = Arc<str>;
pub type TaskId = VertexId;

pub mod collector;
pub mod partition;
#[cfg(test)]
pub mod tests;
