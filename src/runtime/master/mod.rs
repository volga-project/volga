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
mod heartbeat;
mod lifecycle;
mod state;
mod worker_client;

use lifecycle::MasterLifecycle;
use state::MasterState;
pub use events::{CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord};

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
        execution_attempt_id: u64,
    ) -> Result<(), String> {
        self.state
            .report_checkpoint(checkpoint_id, task, blobs, execution_attempt_id)
            .await
    }

    pub async fn report_checkpoint_propagation(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        execution_attempt_id: u64,
        phase: CheckpointPropagationPhase,
    ) -> Result<(), String> {
        self.state
            .report_checkpoint_propagation(checkpoint_id, task, execution_attempt_id, phase)
            .await
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

    /// Begin a checkpoint without going through the run-loop force flag.
    /// Used by tests that drive workers outside `Master::execute`.
    pub async fn start_checkpoint(&self) -> Result<u64, String> {
        self.state
            .begin_checkpoint(self.state.current_attempt_id())
            .await
            .map_err(|error| match error {
                crate::runtime::master::checkpoint::CheckpointStartError::AlreadyInFlight {
                    checkpoint_id,
                } => format!("already in flight checkpoint_id={checkpoint_id}"),
                crate::runtime::master::checkpoint::CheckpointStartError::NoCheckpointableTasks => {
                    "no checkpointable tasks".to_string()
                }
            })
    }

    /// Begin a checkpoint and fan out barrier triggers to active workers immediately.
    pub async fn force_checkpoint(&self) -> Result<u64, String> {
        if !self.state.has_checkpointable_tasks().await {
            return Err("no checkpointable tasks".to_string());
        }
        let attempt_id = self.state.current_attempt_id();
        let checkpoint_id = self
            .state
            .begin_checkpoint(attempt_id)
            .await
            .map_err(|error| match error {
                crate::runtime::master::checkpoint::CheckpointStartError::AlreadyInFlight {
                    checkpoint_id,
                } => format!("already in flight checkpoint_id={checkpoint_id}"),
                crate::runtime::master::checkpoint::CheckpointStartError::NoCheckpointableTasks => {
                    "no checkpointable tasks".to_string()
                }
            })?;
        println!(
            "[MASTER] Force checkpoint {} attempt={}",
            checkpoint_id, attempt_id
        );
        let clients = self.state.open_active_worker_clients().await;
        if clients.is_empty() {
            let _ = self
                .state
                .abort_in_flight_checkpoint(attempt_id, "no active workers".to_string())
                .await;
            return Err("no active workers".to_string());
        }
        for (worker_id, client) in clients {
            match client.trigger_checkpoint_barrier(checkpoint_id).await {
                Ok(true) => {}
                Ok(false) => {
                    let _ = self
                        .state
                        .abort_in_flight_checkpoint(
                            attempt_id,
                            format!("trigger rejected by {worker_id}"),
                        )
                        .await;
                    return Err(format!("trigger rejected by {worker_id}"));
                }
                Err(error) => {
                    let _ = self
                        .state
                        .abort_in_flight_checkpoint(
                            attempt_id,
                            format!("trigger failed on {worker_id}: {error}"),
                        )
                        .await;
                    return Err(format!("trigger failed on {worker_id}: {error}"));
                }
            }
        }
        Ok(checkpoint_id)
    }

    pub async fn stop_sources(&self) -> Result<(), String> {
        let clients = self.state.open_active_worker_clients().await;
        if clients.is_empty() {
            return Err("no active workers".to_string());
        }
        for (worker_id, client) in clients {
            client
                .stop_sources()
                .await
                .map_err(|error| format!("{worker_id}: {error}"))?;
        }
        Ok(())
    }

    pub async fn get_source_stats(&self) -> Result<(Vec<(String, i32, u64)>, u64), String> {
        let clients = self.state.open_active_worker_clients().await;
        if clients.is_empty() {
            return Err("no active workers".to_string());
        }
        let mut tasks = Vec::new();
        let mut total = 0u64;
        for (worker_id, client) in clients {
            let stats = client
                .get_source_stats()
                .await
                .map_err(|error| format!("{worker_id}: {error}"))?;
            for task in stats {
                total += task.records_generated;
                tasks.push((task.vertex_id, task.task_index, task.records_generated));
            }
        }
        Ok((tasks, total))
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
