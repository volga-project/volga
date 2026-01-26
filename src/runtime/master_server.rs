use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::runtime::bootstrap::WorkerBootstrapPayload;

pub mod master_service {
    tonic::include_proto!("master_service");
}

use master_service::{
    master_service_server::MasterService,
    ReportCheckpointRequest, ReportCheckpointResponse,
    GetTaskCheckpointRequest, GetTaskCheckpointResponse, StateBlob,
    GetLatestCompleteCheckpointRequest, GetLatestCompleteCheckpointResponse,
    GetLatestSnapshotRequest, GetLatestSnapshotResponse,
    GetWorkerBootstrapRequest, GetWorkerBootstrapResponse,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskKey {
    pub vertex_id: String,
    pub task_index: i32,
}

#[derive(Debug, Default)]
pub struct MasterCheckpointCoordinator {
    pub acks: HashMap<u64, HashSet<TaskKey>>,
    pub completed: BTreeSet<u64>,
}

impl MasterCheckpointCoordinator {
    pub fn ack(&mut self, checkpoint_id: u64, task: TaskKey, expected_task_count: usize) {
        self.acks.entry(checkpoint_id).or_default().insert(task);
        if expected_task_count != 0 {
            if let Some(acked) = self.acks.get(&checkpoint_id) {
                if acked.len() == expected_task_count {
                    self.completed.insert(checkpoint_id);
                }
            }
        }
    }

    pub fn latest_complete(&self) -> Option<u64> {
        self.completed.iter().next_back().copied()
    }
}

#[derive(Debug, Default)]
pub struct MasterCheckpointStore {
    pub snapshots: HashMap<(u64, TaskKey), Vec<(String, Vec<u8>)>>,
}

impl MasterCheckpointStore {
    pub fn put(&mut self, checkpoint_id: u64, task: TaskKey, blobs: Vec<(String, Vec<u8>)>) {
        self.snapshots.insert((checkpoint_id, task), blobs);
    }
}

#[derive(Debug, Default)]
pub struct MasterCheckpointRegistry {
    pub coordinator: MasterCheckpointCoordinator,
    pub store: MasterCheckpointStore,
    pub expected_tasks: HashSet<TaskKey>,
}

#[derive(Debug, Clone)]
pub struct MasterLatestSnapshot {
    pub ts_ms: u64,
    pub seq: u64,
    pub snapshot_bytes: Vec<u8>,
}

/// Server implementation of MasterService
#[derive(Clone)]
pub struct MasterServiceImpl {
    registry: Arc<Mutex<MasterCheckpointRegistry>>,
    latest_snapshot: Arc<Mutex<Option<MasterLatestSnapshot>>>,
    bootstrap_payload: Arc<Mutex<Option<WorkerBootstrapPayload>>>,
}

impl MasterServiceImpl {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(MasterCheckpointRegistry::default())),
            latest_snapshot: Arc::new(Mutex::new(None)),
            bootstrap_payload: Arc::new(Mutex::new(None)),
        }
    }

    pub fn snapshot_sink(&self) -> Arc<Mutex<Option<MasterLatestSnapshot>>> {
        self.latest_snapshot.clone()
    }

    pub async fn set_bootstrap_payload(&self, payload: WorkerBootstrapPayload) {
        let mut guard = self.bootstrap_payload.lock().await;
        *guard = Some(payload);
    }
}

#[tonic::async_trait]
impl MasterService for MasterServiceImpl {
    async fn report_checkpoint(
        &self,
        request: Request<ReportCheckpointRequest>,
    ) -> Result<Response<ReportCheckpointResponse>, Status> {
        let req = request.into_inner();
        let checkpoint_id = req.checkpoint_id;
        let task = TaskKey {
            vertex_id: req.vertex_id,
            task_index: req.task_index,
        };

        let blobs = req
            .blobs
            .into_iter()
            .map(|b| (b.name, b.bytes))
            .collect::<Vec<_>>();

        let mut registry = self.registry.lock().await;
        registry.store.put(checkpoint_id, task.clone(), blobs);
        let expected_count = registry.expected_tasks.len();
        registry.coordinator.ack(checkpoint_id, task, expected_count);

        Ok(Response::new(ReportCheckpointResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn get_task_checkpoint(
        &self,
        request: Request<GetTaskCheckpointRequest>,
    ) -> Result<Response<GetTaskCheckpointResponse>, Status> {
        let req = request.into_inner();
        let checkpoint_id = req.checkpoint_id;
        let task = TaskKey {
            vertex_id: req.vertex_id,
            task_index: req.task_index,
        };

        let registry = self.registry.lock().await;
        let blobs = registry
            .store
            .snapshots
            .get(&(checkpoint_id, task))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(name, bytes)| StateBlob { name, bytes })
            .collect::<Vec<_>>();

        Ok(Response::new(GetTaskCheckpointResponse {
            success: true,
            error_message: String::new(),
            blobs,
        }))
    }

    async fn get_latest_complete_checkpoint(
        &self,
        _request: Request<GetLatestCompleteCheckpointRequest>,
    ) -> Result<Response<GetLatestCompleteCheckpointResponse>, Status> {
        let registry = self.registry.lock().await;
        if let Some(checkpoint_id) = registry.coordinator.latest_complete() {
            Ok(Response::new(GetLatestCompleteCheckpointResponse {
                success: true,
                error_message: String::new(),
                has_checkpoint: true,
                checkpoint_id,
            }))
        } else {
            Ok(Response::new(GetLatestCompleteCheckpointResponse {
                success: true,
                error_message: String::new(),
                has_checkpoint: false,
                checkpoint_id: 0,
            }))
        }
    }

    async fn get_latest_snapshot(
        &self,
        _request: Request<GetLatestSnapshotRequest>,
    ) -> Result<Response<GetLatestSnapshotResponse>, Status> {
        let guard = self.latest_snapshot.lock().await;
        if let Some(s) = guard.as_ref() {
            Ok(Response::new(GetLatestSnapshotResponse {
                has_snapshot: true,
                snapshot_bytes: s.snapshot_bytes.clone(),
                ts_ms: s.ts_ms,
                seq: s.seq,
            }))
        } else {
            Ok(Response::new(GetLatestSnapshotResponse {
                has_snapshot: false,
                snapshot_bytes: Vec::new(),
                ts_ms: 0,
                seq: 0,
            }))
        }
    }

    async fn get_worker_bootstrap(
        &self,
        _request: Request<GetWorkerBootstrapRequest>,
    ) -> Result<Response<GetWorkerBootstrapResponse>, Status> {
        let guard = self.bootstrap_payload.lock().await;
        let Some(payload) = guard.as_ref() else {
            return Err(Status::failed_precondition("bootstrap payload not set"));
        };
        let bytes = bincode::serialize(payload).map_err(|e| {
            Status::internal(format!("failed to serialize bootstrap payload: {e}"))
        })?;
        Ok(Response::new(GetWorkerBootstrapResponse { bootstrap_bytes: bytes }))
    }
}

/// Server that hosts MasterService
pub struct MasterServer {
    service: MasterServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MasterServer {
    pub fn new() -> Self {
        Self {
            service: MasterServiceImpl::new(),
            server_handle: None,
            shutdown_sender: None,
        }
    }

    pub fn snapshot_sink(&self) -> Arc<Mutex<Option<MasterLatestSnapshot>>> {
        self.service.snapshot_sink()
    }

    pub async fn set_bootstrap_payload(&mut self, payload: WorkerBootstrapPayload) {
        self.service.set_bootstrap_payload(payload).await;
    }

    pub async fn set_checkpointable_tasks(&mut self, tasks: Vec<TaskKey>) {
        let mut registry = self.service.registry.lock().await;
        registry.expected_tasks = tasks.into_iter().collect();
    }

    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
        let addr = addr.parse()?;
        let service = master_service::master_service_server::MasterServiceServer::new(self.service.clone());

        println!("[MASTER_SERVER] Starting MasterService server on {}", addr);

        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    shutdown_receiver.await.ok();
                })
                .await;
        });

        self.server_handle = Some(server_handle);
        self.shutdown_sender = Some(shutdown_sender);
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(shutdown_sender) = self.shutdown_sender.take() {
            let _ = shutdown_sender.send(());
        }
        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
    }
}


