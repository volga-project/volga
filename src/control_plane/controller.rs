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
use crate::executor::runtime_adapter::{RuntimeAdapter, StartAttemptRequest};
use crate::api::spec::runtime_adapter::RuntimeAdapterSpec;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::api::PipelineSpec as UserPipelineSpec;
use crate::runtime::observability::{PipelineSnapshot, PipelineSnapshotEntry};
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master_server::master_service::GetLatestSnapshotRequest;

#[derive(Clone)]
pub struct ControlPlaneController {
    store: Arc<InMemoryStore>,
    adapters: HashMap<RuntimeAdapterSpec, Arc<dyn RuntimeAdapter>>,
    graphs: Arc<RwLock<HashMap<PipelineId, ExecutionGraph>>>,
    specs: Arc<RwLock<HashMap<PipelineId, UserPipelineSpec>>>,
    running: Arc<Mutex<HashMap<PipelineId, RunningAttempt>>>,
    snapshot_pollers: Arc<Mutex<HashMap<PipelineId, JoinHandle<()>>>>,
}

#[derive(Clone)]
struct RunningAttempt {
    adapter_spec: RuntimeAdapterSpec,
    handle: AttemptHandle,
}

impl ControlPlaneController {
    pub fn new(
        store: Arc<InMemoryStore>,
        adapters: HashMap<RuntimeAdapterSpec, Arc<dyn RuntimeAdapter>>,
    ) -> Self {
        Self {
            store,
            adapters,
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
            let pipeline_id = run.pipeline_execution_context.pipeline_id;
            let desired = self
                .store
                .get_desired_state(pipeline_id)
                .await
                .unwrap_or(run.desired_state);

            let mut running_guard = self.running.lock().await;
            let mut pollers_guard = self.snapshot_pollers.lock().await;

            match desired {
                PipelineDesiredState::Running => {
                    if let Some(running) = running_guard.get(&pipeline_id) {
                        if running.handle.is_finished() {
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

                    let adapter_spec = spec
                        .execution_profile
                        .runtime_adapter_spec()
                        .unwrap_or_else(|| panic!("execution profile does not use a runtime adapter"));
                    let adapter = self
                        .adapters
                        .get(&adapter_spec)
                        .unwrap_or_else(|| panic!("missing runtime adapter for {:?}", adapter_spec));

                    let mut spec = spec.clone();
                    spec.worker_runtime =
                        crate::runtime::execution_plan::ExecutionPlan::resolve_worker_runtime(&spec);
                    let handle = adapter
                        .start_attempt(StartAttemptRequest {
                            pipeline_execution_context: run.pipeline_execution_context.clone(),
                            pipeline_spec: spec,
                            execution_graph: graph,
                        })
                        .await?;

                    self.store
                        .put_status(
                            pipeline_id,
                            PipelineStatus {
                                pipeline_execution_context: run.pipeline_execution_context.clone(),
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
                                pipeline_execution_context: run.pipeline_execution_context.clone(),
                                at: Utc::now(),
                                kind: PipelineEventKind::StateChanged {
                                    state: PipelineLifecycleState::Running,
                                },
                            },
                        )
                        .await;

                    running_guard.insert(
                        pipeline_id,
                        RunningAttempt {
                            adapter_spec: adapter_spec.clone(),
                            handle,
                        },
                    );

                    // Start snapshot polling (CP -> Master) for this pipeline attempt.
                    if !pollers_guard.contains_key(&pipeline_id) {
                        let store = self.store.clone();
                        let master_addr = running_guard
                            .get(&pipeline_id)
                            .expect("handle just inserted")
                            .handle
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
                    if let Some(running) = running_guard.remove(&pipeline_id) {
                        let adapter = self
                            .adapters
                            .get(&running.adapter_spec)
                            .unwrap_or_else(|| panic!("missing runtime adapter for {:?}", running.adapter_spec));
                        adapter.stop_attempt(running.handle).await?;
                    }
                    if let Some(h) = pollers_guard.remove(&pipeline_id) {
                        h.abort();
                    }

                    self.store
                        .put_status(
                            pipeline_id,
                            PipelineStatus {
                                pipeline_execution_context: run.pipeline_execution_context.clone(),
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
                                pipeline_execution_context: run.pipeline_execution_context.clone(),
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

