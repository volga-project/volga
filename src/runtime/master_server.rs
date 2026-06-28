use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use crate::orchestrator::orchestrator::Orchestrator;
use crate::runtime::master::{Master, MasterConfig};
use crate::runtime::master_checkpoint::TaskKey;
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};

pub mod master_service {
    tonic::include_proto!("master_service");
}

use master_service::{
    master_service_server::MasterService,
    RegisterWorkerRequest, RegisterWorkerResponse,
    ReportCheckpointRequest, ReportCheckpointResponse,
    GetTaskCheckpointRequest, GetTaskCheckpointResponse, StateBlob,
    GetLatestCompleteCheckpointRequest, GetLatestCompleteCheckpointResponse,
    GetLatestPipelineSnapshotRequest, GetLatestPipelineSnapshotResponse,
};

/// Server implementation of MasterService
#[derive(Clone)]
pub struct MasterServiceImpl {
    master: Arc<Master>,
}

impl MasterServiceImpl {
    pub fn new(orchestrator: Arc<dyn Orchestrator>) -> Self {
        Self {
            master: Arc::new(Master::new(orchestrator)),
        }
    }
}

#[tonic::async_trait]
impl MasterService for MasterServiceImpl {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        self.master.register_worker(req.worker_id).await;
        Ok(Response::new(RegisterWorkerResponse {
            success: true,
            error_message: String::new(),
        }))
    }

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
        self.master
            .report_checkpoint(checkpoint_id, task, blobs)
            .await;

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

        let blobs = self
            .master
            .get_task_checkpoint(checkpoint_id, task)
            .await
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
        if let Some(checkpoint_id) = self.master.get_latest_complete_checkpoint().await {
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

    async fn get_latest_pipeline_snapshot(
        &self,
        _request: Request<GetLatestPipelineSnapshotRequest>,
    ) -> Result<Response<GetLatestPipelineSnapshotResponse>, Status> {
        let snapshot_opt: Option<PipelineSnapshot> = self.master.get_latest_pipeline_snapshot().await;

        if let Some(snapshot) = snapshot_opt {
            let snapshot_bytes = bincode::serialize(&snapshot).map_err(|e| {
                Status::internal(format!("Failed to serialize latest pipeline snapshot: {}", e))
            })?;
            Ok(Response::new(GetLatestPipelineSnapshotResponse {
                has_snapshot: true,
                snapshot_bytes,
                ts_ms: 0,
                seq: 0,
            }))
        } else {
            Ok(Response::new(GetLatestPipelineSnapshotResponse {
                has_snapshot: false,
                snapshot_bytes: Vec::new(),
                ts_ms: 0,
                seq: 0,
            }))
        }
    }
}

/// Server that hosts MasterService
pub struct MasterServer {
    service: MasterServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MasterServer {
    pub fn new(orchestrator: Arc<dyn Orchestrator>) -> Self {
        Self {
            service: MasterServiceImpl::new(orchestrator),
            server_handle: None,
            shutdown_sender: None,
        }
    }

    pub async fn configure(&self, config: MasterConfig) {
        self.service.master.configure(config).await;
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.service.master.execute().await
    }

    pub async fn get_worker_states(&self) -> HashMap<String, WorkerSnapshot> {
        self.service.master.get_worker_states().await
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

