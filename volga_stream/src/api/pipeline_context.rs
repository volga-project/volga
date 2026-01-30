use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::sync::Mutex;

use crate::api::{ExecutionProfile, PipelineSpec};
use crate::control_plane::types::{AttemptId, PipelineExecutionContext};
use crate::executor::local_runtime_adapter::LocalRuntimeAdapter;
use crate::executor::runtime_adapter::{RuntimeAdapter, StartAttemptRequest};
use crate::api::spec::runtime_adapter::RuntimeAdapterSpec;
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::PipelineSnapshotHistory;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;

#[derive(Clone)]
pub struct PipelineContext {
    pub spec: PipelineSpec,
    runtime_adapters: StdHashMap<RuntimeAdapterSpec, Arc<dyn RuntimeAdapter>>,
}

pub struct PipelineContextRunHandle {
    pub snapshots: mpsc::Receiver<PipelineSnapshot>,
    pub history: Arc<Mutex<PipelineSnapshotHistory>>,
    pub join: JoinHandle<Result<PipelineSnapshot>>,
}

impl PipelineContextRunHandle {
    pub async fn wait(self) -> Result<PipelineSnapshot> {
        self.join.await.expect("pipeline join")
    }
}

impl PipelineContext {
    pub fn new(spec: PipelineSpec) -> Self {
        Self {
            spec,
            runtime_adapters: StdHashMap::new(),
        }
    }

    pub fn with_runtime_adapters(
        mut self,
        adapters: StdHashMap<RuntimeAdapterSpec, Arc<dyn RuntimeAdapter>>,
    ) -> Self {
        self.runtime_adapters = adapters;
        self
    }

    pub async fn execute_with_state_updates(
        self,
        state_updates_sender: Option<mpsc::Sender<PipelineSnapshot>>,
    ) -> Result<PipelineSnapshot> {
        let mut execution_plan =
            crate::runtime::execution_plan::ExecutionPlan::from_spec(&self.spec, Vec::new());
        let mut execution_graph = execution_plan.execution_graph.clone();
        let pipeline_execution_context = PipelineExecutionContext::fresh(AttemptId(1));

        let mut spec = self.spec.clone();
        spec.worker_runtime = execution_plan.worker_runtime.clone();

        match self.spec.execution_profile.clone() {
            ExecutionProfile::InProcess => {
                let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();
                let worker_id = "single_worker".to_string();

                let worker_config = WorkerConfig::new(
                    worker_id.clone(),
                    pipeline_execution_context,
                    execution_graph,
                    vertex_ids,
                    TransportBackendType::InMemory,
                    spec.worker_runtime,
                );
                let mut worker = Worker::new(worker_config);

                if let Some(pipeline_state_sender) = state_updates_sender {
                    let (worker_state_sender, mut worker_state_receiver) =
                        mpsc::channel::<WorkerSnapshot>(100);
                    let pipeline_sender = pipeline_state_sender.clone();
                    let worker_id_clone = worker_id.clone();
                    tokio::spawn(async move {
                        while let Some(worker_state) = worker_state_receiver.recv().await {
                            let mut worker_states = StdHashMap::new();
                            worker_states.insert(worker_id_clone.clone(), worker_state);
                            let pipeline_state = PipelineSnapshot::new(worker_states);
                            let _ = pipeline_sender.send(pipeline_state).await;
                        }
                    });

                    worker
                        .execute_worker_lifecycle_for_testing_with_state_updates(
                            worker_state_sender,
                        )
                        .await;
                } else {
                    worker.execute_worker_lifecycle_for_testing().await;
                }

                let worker_state = worker.get_state().await;
                worker.close().await;

                let mut worker_states = StdHashMap::new();
                worker_states.insert(worker_id, worker_state);
                Ok(PipelineSnapshot::new(worker_states))
            }
            ExecutionProfile::Local { .. } => {
                let adapter = Arc::new(LocalRuntimeAdapter::new());

                let handle = adapter
                    .start_attempt(StartAttemptRequest {
                        pipeline_execution_context,
                        pipeline_spec: spec.clone(),
                        execution_graph,
                    })
                    .await?;

                let final_state = handle.wait().await?;
                if let Some(sender) = state_updates_sender {
                    let _ = sender.send(final_state.clone()).await;
                }
                Ok(final_state)
            }
            ExecutionProfile::K8s { .. } | ExecutionProfile::Custom { .. } => {
                let adapter_spec = self
                    .spec
                    .execution_profile
                    .runtime_adapter_spec()
                    .unwrap_or_else(|| panic!("execution profile does not use a runtime adapter"));
                let runtime_adapter = self
                    .runtime_adapters
                    .get(&adapter_spec)
                    .unwrap_or_else(|| panic!("missing runtime adapter for {:?}", adapter_spec));

                let handle = runtime_adapter
                    .start_attempt(StartAttemptRequest {
                        pipeline_execution_context,
                        pipeline_spec: spec.clone(),
                        execution_graph,
                    })
                    .await?;

                let final_state = handle.wait().await?;
                if let Some(sender) = state_updates_sender {
                    let _ = sender.send(final_state.clone()).await;
                }
                Ok(final_state)
            }
        }
    }

    pub async fn execute(self) -> Result<PipelineSnapshot> {
        self.execute_with_state_updates(None).await
    }

    /// Execute the pipeline and stream intermediate snapshots over a channel.
    ///
    /// This is primarily for harness/unit/in-proc usage; orchestrated mode should use the Control Plane.
    pub fn execute_with_snapshot_stream(self, channel_capacity: usize) -> (mpsc::Receiver<PipelineSnapshot>, JoinHandle<Result<PipelineSnapshot>>) {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        let handle = tokio::spawn(async move { self.execute_with_state_updates(Some(tx)).await });
        (rx, handle)
    }

    /// Execute the pipeline and maintain an in-memory snapshot history (bounded by retention policy).
    pub fn execute_with_snapshot_history(self, channel_capacity: usize) -> PipelineContextRunHandle {
        let cap = channel_capacity.max(1);

        let (inner_tx, mut inner_rx) = mpsc::channel::<PipelineSnapshot>(cap);
        let (outer_tx, outer_rx) = mpsc::channel::<PipelineSnapshot>(cap);

        let retention = self.spec.worker_runtime.history_retention_window();
        let history = Arc::new(Mutex::new(PipelineSnapshotHistory::new(retention)));
        let history_for_forwarder = history.clone();

        let forwarder = tokio::spawn(async move {
            while let Some(snap) = inner_rx.recv().await {
                history_for_forwarder.lock().await.push_now(snap.clone());
                let _ = outer_tx.send(snap).await;
            }
        });

        let join = tokio::spawn(async move {
            let res = self.execute_with_state_updates(Some(inner_tx)).await;
            let _ = forwarder.await;
            res
        });

        PipelineContextRunHandle {
            snapshots: outer_rx,
            history,
            join,
        }
    }
}