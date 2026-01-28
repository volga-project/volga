use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::control_plane::store::{InMemoryStore, PipelineEventStore, PipelineRunStore, PipelineSnapshotStore};
use crate::control_plane::types::{
    PipelineDesiredState, PipelineEvent, PipelineEventKind, PipelineId, PipelineLifecycleState,
    PipelineStatus,
};
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::api::PipelineSpec as UserPipelineSpec;
use crate::runtime::observability::{PipelineSnapshot, PipelineSnapshotEntry};
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::master_service::GetLatestSnapshotRequest;

#[derive(Clone)]
pub struct ControlPlaneController {
    store: Arc<InMemoryStore>,
    adapter: Arc<dyn RuntimeAdapter>,
    graphs: Arc<RwLock<HashMap<PipelineId, ExecutionGraph>>>,
    specs: Arc<RwLock<HashMap<PipelineId, UserPipelineSpec>>>,
    running: Arc<Mutex<HashMap<PipelineId, AttemptHandle>>>,
    snapshot_pollers: Arc<Mutex<HashMap<PipelineId, JoinHandle<()>>>>,
}

impl ControlPlaneController {
    pub fn new(store: Arc<InMemoryStore>, adapter: Arc<dyn RuntimeAdapter>) -> Self {
        Self {
            store,
            adapter,
            graphs: Arc::new(RwLock::new(HashMap::new())),
            specs: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
            snapshot_pollers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_execution_graph(&self, pipeline_id: PipelineId, graph: ExecutionGraph) {
        let mut guard = self.graphs.write().await;
        guard.insert(pipeline_id, graph);
    }

    pub async fn register_pipeline_spec(&self, pipeline_id: PipelineId, spec: UserPipelineSpec) {
        let mut guard = self.specs.write().await;
        guard.insert(pipeline_id, spec);
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
        let specs_guard = self.specs.read().await;

        for run in runs {
            let pipeline_id = run.execution_ids.pipeline_id;
            let desired = self
                .store
                .get_desired_state(pipeline_id)
                .await
                .unwrap_or(run.desired_state);

            let mut running_guard = self.running.lock().await;
            let mut pollers_guard = self.snapshot_pollers.lock().await;

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

                    let spec = specs_guard
                        .get(&pipeline_id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("missing pipeline spec for pipeline_id={:?}", pipeline_id))?;

                    let num_workers_per_operator = match spec.execution_profile {
                        crate::api::ExecutionProfile::Orchestrated { num_workers_per_operator } => num_workers_per_operator,
                        _ => 1,
                    };

                    let handle = self
                        .adapter
                        .start_attempt(StartAttemptRequest {
                            execution_ids: run.execution_ids.clone(),
                            pipeline_spec: spec.clone(),
                            execution_graph: graph,
                            num_workers_per_operator,
                            node_assign_strategy: spec.node_assign_strategy.clone(),
                            transport_overrides_queue_records: spec.transport_overrides_queue_records(),
                            worker_runtime: spec.worker_runtime.clone(),
                            operator_type_storage_overrides: spec.operator_type_storage_overrides(),
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

                    // Start snapshot polling (CP -> Master) for this pipeline attempt.
                    if !pollers_guard.contains_key(&pipeline_id) {
                        let store = self.store.clone();
                        let master_addr = running_guard
                            .get(&pipeline_id)
                            .expect("handle just inserted")
                            .master_addr
                            .clone();
                        let poll_pipeline_id = pipeline_id;

                        let poller = tokio::spawn(async move {
                            let addr = format!("http://{}", master_addr);
                            let mut client = match MasterServiceClient::connect(addr).await {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("[CONTROL_PLANE] snapshot poller connect failed: {e:?}");
                                    return;
                                }
                            };

                            loop {
                                match client
                                    .get_latest_snapshot(tonic::Request::new(GetLatestSnapshotRequest {}))
                                    .await
                                {
                                    Ok(resp) => {
                                        let resp = resp.into_inner();
                                        if !resp.has_snapshot || resp.snapshot_bytes.is_empty() {
                                            sleep(Duration::from_millis(200)).await;
                                            continue;
                                        }
                                        let snapshot: PipelineSnapshot = match bincode::deserialize(&resp.snapshot_bytes) {
                                            Ok(s) => s,
                                            Err(e) => {
                                                eprintln!("[CONTROL_PLANE] snapshot decode failed: {e:?}");
                                                sleep(Duration::from_millis(200)).await;
                                                continue;
                                            }
                                        };
                                        store
                                            .append_snapshot(
                                                poll_pipeline_id,
                                                PipelineSnapshotEntry {
                                                    ts_ms: resp.ts_ms,
                                                    seq: resp.seq,
                                                    snapshot,
                                                },
                                            )
                                            .await;
                                    }
                                    Err(e) => {
                                        eprintln!("[CONTROL_PLANE] snapshot poller rpc failed: {e:?}");
                                        break;
                                    }
                                }
                                sleep(Duration::from_millis(200)).await;
                            }
                        });

                        pollers_guard.insert(pipeline_id, poller);
                    }
                }
                PipelineDesiredState::Stopped | PipelineDesiredState::Paused | PipelineDesiredState::Draining => {
                    if let Some(handle) = running_guard.remove(&pipeline_id) {
                        self.adapter.stop_attempt(handle).await?;
                    }
                    if let Some(h) = pollers_guard.remove(&pipeline_id) {
                        h.abort();
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

