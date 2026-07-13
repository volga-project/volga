use std::collections::HashMap;
use std::sync::Arc;

use crate::api::PipelineSpec;
use crate::orchestrator::orchestrator::MasterOrchestrator;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::master::checkpoint::TaskKey;
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

pub mod checkpoint;
pub mod events;
pub mod server;
mod service;

mod attempt;
mod failure;
mod heartbeat;
mod lifecycle;
mod state;
mod worker_client;

use lifecycle::MasterLifecycle;
use state::MasterState;
pub use events::{LifecycleEvent, LifecycleEventRecord};

/// Master that orchestrates multiple worker servers.
pub struct Master {
    state: Arc<MasterState>,
}

#[derive(Clone)]
pub struct MasterConfig {
    pub spec: Option<PipelineSpec>,
    pub execution_graph: ExecutionGraph,
    pub expected_workers: usize,
}

impl MasterConfig {
    pub fn with_spec(
        spec: PipelineSpec,
        execution_graph: ExecutionGraph,
        expected_workers: usize,
    ) -> Self {
        Self {
            spec: Some(spec),
            execution_graph,
            expected_workers,
        }
    }

    pub fn with_graph(execution_graph: ExecutionGraph, expected_workers: usize) -> Self {
        Self {
            spec: None,
            execution_graph,
            expected_workers,
        }
    }
}

impl Master {
    pub fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            state: Arc::new(MasterState::new(orchestrator)),
        }
    }

    pub async fn register_worker(&self, worker_id: String) {
        self.state.register_worker(worker_id).await;
    }

    pub fn checkpointable_tasks_for_graph(execution_graph: &ExecutionGraph) -> Vec<TaskKey> {
        MasterState::checkpointable_tasks_for_graph(execution_graph)
    }

    pub async fn configure(&self, config: MasterConfig) {
        self.state.configure(config).await;
    }

    pub async fn report_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) {
        self.state
            .report_checkpoint(checkpoint_id, task, blobs)
            .await;
    }

    pub async fn get_task_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> Vec<(String, Vec<u8>)> {
        self.state.task_checkpoint(checkpoint_id, task).await
    }

    pub async fn get_latest_complete_checkpoint(&self) -> Option<u64> {
        self.state.latest_complete_checkpoint().await
    }

    pub async fn get_worker_states(&self) -> HashMap<String, WorkerSnapshot> {
        self.state.worker_states().await
    }

    pub async fn get_latest_pipeline_snapshot(&self) -> Option<PipelineSnapshot> {
        self.state.latest_pipeline_snapshot().await
    }

    pub async fn lifecycle_events_since(&self, sequence: u64) -> Vec<LifecycleEventRecord> {
        self.state.lifecycle_events_since(sequence).await
    }

    pub fn subscribe_lifecycle_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<LifecycleEventRecord> {
        self.state.subscribe_lifecycle_events()
    }

    /// Run execution attempts until the pipeline finishes or recovery is exhausted.
    pub async fn execute(&self) -> anyhow::Result<()> {
        let pipeline = self.state.pipeline_context().await?;
        MasterLifecycle::new(self.state.clone()).run(pipeline).await
    }
}
