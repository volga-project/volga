use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::sync::Mutex;

use crate::api::{compile_logical_graph, ExecutionProfile, PipelineSpec};
use crate::control_plane::types::{AttemptId, ExecutionIds};
use crate::executor::local_runtime_adapter::LocalRuntimeAdapter;
use crate::executor::runtime_adapter::{RuntimeAdapter, StartAttemptRequest};
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::PipelineSnapshotHistory;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;

#[derive(Clone)]
pub struct PipelineContext {
    pub spec: PipelineSpec,
    runtime_adapter: Option<Arc<dyn RuntimeAdapter>>,
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
            runtime_adapter: None,
        }
    }

    pub fn with_runtime_adapter(mut self, adapter: Arc<dyn RuntimeAdapter>) -> Self {
        self.runtime_adapter = Some(adapter);
        self
    }

    pub async fn execute_with_state_updates(
        self,
        state_updates_sender: Option<mpsc::Sender<PipelineSnapshot>>,
    ) -> Result<PipelineSnapshot> {
        let logical_graph = compile_logical_graph(&self.spec);
        let mut execution_graph = logical_graph.to_execution_graph();
        let execution_ids = ExecutionIds::fresh(AttemptId(1));

        match self.spec.execution_profile.clone() {
            ExecutionProfile::InProcess => {
                execution_graph.update_channels_with_node_mapping_and_transport(
                    None,
                    &self.spec.worker_runtime.transport,
                    &self.spec.transport_overrides_queue_records(),
                );
                let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();
                let worker_id = "single_worker".to_string();

                let mut worker_config = WorkerConfig::new(
                    worker_id.clone(),
                    execution_ids,
                    execution_graph,
                    vertex_ids,
                    self.spec.worker_runtime.num_threads_per_task,
                    TransportBackendType::InMemory,
                );
                worker_config.storage_budgets = self.spec.worker_runtime.storage.budgets.clone();
                worker_config.inmem_store_lock_pool_size = self.spec.worker_runtime.storage.inmem_store_lock_pool_size;
                worker_config.inmem_store_bucket_granularity = self.spec.worker_runtime.storage.inmem_store_bucket_granularity;
                worker_config.inmem_store_max_batch_size = self.spec.worker_runtime.storage.inmem_store_max_batch_size;
                worker_config.operator_type_storage_overrides = self.spec.operator_type_storage_overrides();
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
                        execution_ids,
                        pipeline_spec: self.spec.clone(),
                        execution_graph,
                        transport_overrides_queue_records: self.spec.transport_overrides_queue_records(),
                        worker_runtime: self.spec.worker_runtime.clone(),
                        operator_type_storage_overrides: self.spec.operator_type_storage_overrides(),
                    })
                    .await?;

                let final_state = handle.wait().await?;
                if let Some(sender) = state_updates_sender {
                    let _ = sender.send(final_state.clone()).await;
                }
                Ok(final_state)
            }
            ExecutionProfile::K8s { .. } | ExecutionProfile::Custom { .. } => {
                let runtime_adapter = self
                    .runtime_adapter
                    .expect("Execution profile requires a runtime_adapter");
                let adapter_spec = self
                    .spec
                    .execution_profile
                    .runtime_adapter_spec()
                    .unwrap_or_else(|| panic!("execution profile does not use a runtime adapter"));
                if runtime_adapter.runtime_kind() != adapter_spec.kind() {
                    panic!(
                        "runtime adapter kind {:?} does not match pipeline spec {:?}",
                        runtime_adapter.runtime_kind(),
                        adapter_spec.kind()
                    );
                }

                let handle = runtime_adapter
                    .start_attempt(StartAttemptRequest {
                        execution_ids,
                        pipeline_spec: self.spec.clone(),
                        execution_graph,
                        transport_overrides_queue_records: self.spec.transport_overrides_queue_records(),
                        worker_runtime: self.spec.worker_runtime.clone(),
                        operator_type_storage_overrides: self.spec.operator_type_storage_overrides(),
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