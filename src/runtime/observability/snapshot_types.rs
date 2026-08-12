use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::common::types::PipelineId;
use crate::runtime::metrics::WorkerAggregateMetrics;
use crate::runtime::operators::window::metrics::WindowOperatorMetricsSnapshot;
use crate::runtime::VertexId;
use anyhow::Result;
use super::task_metadata::TaskMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamTaskStatus {
    Created = 0,
    Opened = 1,
    Running = 2,
    Finished = 3,
    Closed = 4,
}

impl From<u8> for StreamTaskStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => StreamTaskStatus::Created,
            1 => StreamTaskStatus::Opened,
            2 => StreamTaskStatus::Running,
            3 => StreamTaskStatus::Finished,
            4 => StreamTaskStatus::Closed,
            _ => panic!("Invalid task status value"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub vertex_id: VertexId,
    pub status: StreamTaskStatus,
    /// Opaque operator data for harness/debug (e.g. source `records_generated`).
    pub metadata: TaskMetadata,
}

/// Per-task operator metrics for the API snapshot.
///
/// Built on the worker poll path from the in-process metrics registry visit
/// (op collectors). Hot-path writes still go through the metrics fanout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskOperatorMetrics {
    Window(WindowOperatorMetricsSnapshot),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    pub pipeline_id: PipelineId,
    pub worker_id: String,
    pub task_statuses: HashMap<VertexId, StreamTaskStatus>,
    pub worker_metrics: Option<WorkerAggregateMetrics>,
    pub task_operator_metrics: HashMap<VertexId, TaskOperatorMetrics>,
    pub task_metadata: HashMap<VertexId, TaskMetadata>,
}

impl WorkerSnapshot {
    pub fn new(worker_id: String, pipeline_id: PipelineId) -> Self {
        Self {
            pipeline_id,
            worker_id,
            task_statuses: HashMap::new(),
            worker_metrics: None,
            task_operator_metrics: HashMap::new(),
            task_metadata: HashMap::new(),
        }
    }

    /// True when there is at least one task and every task status is in `allowed`.
    pub fn all_tasks_in(&self, allowed: &[StreamTaskStatus]) -> bool {
        !self.task_statuses.is_empty()
            && self
                .task_statuses
                .values()
                .all(|status| allowed.contains(status))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }

    pub fn set_metrics(&mut self, worker_metrics: WorkerAggregateMetrics) {
        self.worker_metrics = Some(worker_metrics);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub worker_states: HashMap<String, WorkerSnapshot>,
}

impl PipelineSnapshot {
    pub fn new(worker_states: HashMap<String, WorkerSnapshot>) -> Self {
        Self { worker_states }
    }
}
