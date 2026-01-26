pub mod runtime_context;
pub mod execution_graph;
pub mod operators;
pub mod stream_task;
pub mod stream_task_actor;
pub mod worker;
pub mod worker_server;
pub mod master;
pub mod master_server;
pub mod bootstrap;
pub mod functions;
pub mod metrics;
pub mod observability;
pub mod state;
pub mod utils;
pub mod watermark;

use std::sync::Arc;

/// Operator/task identity within a worker.
///
/// In this codebase `vertex_id` is effectively the task namespace (unique per operator task).
pub type VertexId = Arc<str>;
pub type TaskId = VertexId;

pub mod partition;
pub mod collector;
#[cfg(test)]
pub mod tests;