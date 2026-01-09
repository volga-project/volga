use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::control_plane::store::{InMemoryStore, PipelineEventStore, PipelineRunStore};
use crate::control_plane::types::{
    PipelineDesiredState, PipelineEvent, PipelineEventKind, PipelineId, PipelineLifecycleState,
    PipelineStatus,
};
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::cluster::cluster_provider::LocalMachineClusterProvider;
use crate::cluster::node_assignment::SingleNodeStrategy;

#[derive(Clone)]
pub struct ControlPlaneController {
    store: Arc<InMemoryStore>,
    adapter: Arc<dyn RuntimeAdapter>,
    graphs: Arc<RwLock<HashMap<PipelineId, ExecutionGraph>>>,
    running: Arc<Mutex<HashMap<PipelineId, AttemptHandle>>>,
}

impl ControlPlaneController {
    pub fn new(store: Arc<InMemoryStore>, adapter: Arc<dyn RuntimeAdapter>) -> Self {
        Self {
            store,
            adapter,
            graphs: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_execution_graph(&self, pipeline_id: PipelineId, graph: ExecutionGraph) {
        let mut guard = self.graphs.write().await;
        guard.insert(pipeline_id, graph);
    }

    pub fn start_reconciler(self, poll_interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.reconcile_once().await {
                    eprintln!("[CONTROL_PLANE] reconcile error: {e:?}");
                }
                sleep(poll_interval).await;
            }
        })
    }

    pub async fn reconcile_once(&self) -> Result<()> {
        let runs = self.store.list_runs().await;
        let graphs_guard = self.graphs.read().await;

        for run in runs {
            let pipeline_id = run.execution_ids.pipeline_id;
            let desired = self
                .store
                .get_desired_state(pipeline_id)
                .await
                .unwrap_or(run.desired_state);

            let mut running_guard = self.running.lock().await;

            match desired {
                PipelineDesiredState::Running => {
                    if let Some(handle) = running_guard.get(&pipeline_id) {
                        if handle.is_finished() {
                            running_guard.remove(&pipeline_id);
                        } else {
                            continue;
                        }
                    }

                    let graph = graphs_guard
                        .get(&pipeline_id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("missing execution graph for pipeline_id={:?}", pipeline_id))?;

                    let handle = self
                        .adapter
                        .start_attempt(StartAttemptRequest {
                            execution_ids: run.execution_ids.clone(),
                            execution_graph: graph,
                            num_workers_per_operator: 1,
                            cluster_provider: Arc::new(LocalMachineClusterProvider::single_node()),
                            node_assign: Arc::new(SingleNodeStrategy),
                        })
                        .await?;

                    self.store
                        .put_status(
                            pipeline_id,
                            PipelineStatus {
                                execution_ids: run.execution_ids.clone(),
                                state: PipelineLifecycleState::Running,
                                updated_at: Utc::now(),
                                worker_count: handle.worker_addrs.len(),
                                task_count: 0,
                                last_checkpoint_id: None,
                            },
                        )
                        .await;
                    self.store
                        .append_event(
                            pipeline_id,
                            PipelineEvent {
                                execution_ids: run.execution_ids.clone(),
                                at: Utc::now(),
                                kind: PipelineEventKind::StateChanged {
                                    state: PipelineLifecycleState::Running,
                                },
                            },
                        )
                        .await;

                    running_guard.insert(pipeline_id, handle);
                }
                PipelineDesiredState::Stopped | PipelineDesiredState::Paused | PipelineDesiredState::Draining => {
                    if let Some(handle) = running_guard.remove(&pipeline_id) {
                        handle.abort();
                    }

                    self.store
                        .put_status(
                            pipeline_id,
                            PipelineStatus {
                                execution_ids: run.execution_ids.clone(),
                                state: PipelineLifecycleState::Stopped,
                                updated_at: Utc::now(),
                                worker_count: 0,
                                task_count: 0,
                                last_checkpoint_id: None,
                            },
                        )
                        .await;
                    self.store
                        .append_event(
                            pipeline_id,
                            PipelineEvent {
                                execution_ids: run.execution_ids.clone(),
                                at: Utc::now(),
                                kind: PipelineEventKind::StateChanged {
                                    state: PipelineLifecycleState::Stopped,
                                },
                            },
                        )
                        .await;
                }
            }
        }

        Ok(())
    }
}

