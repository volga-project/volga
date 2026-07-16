use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::orchestrator::orchestrator::MasterOrchestrator;
use crate::runtime::master::checkpoint::TaskKey;
use crate::runtime::master::server::master_service::{
    master_service_server::MasterService, CheckpointPropagationPhase as ProtoPropagationPhase,
    GetLatestCompleteCheckpointRequest, GetLatestCompleteCheckpointResponse,
    GetLatestPipelineSnapshotRequest, GetLatestPipelineSnapshotResponse,
    GetLifecycleEventsRequest, GetLifecycleEventsResponse, GetSourceStatsRequest,
    GetSourceStatsResponse, GetTaskCheckpointRequest, GetTaskCheckpointResponse,
    LifecycleEventRecord, RegisterWorkerRequest, RegisterWorkerResponse,
    ReportCheckpointPropagationRequest, ReportCheckpointPropagationResponse,
    ReportCheckpointRequest, ReportCheckpointResponse, SourceTaskStats, StateBlob,
    StopSourcesRequest, StopSourcesResponse, TriggerCheckpointRequest, TriggerCheckpointResponse,
};
use crate::runtime::master::{CheckpointPropagationPhase, Master};
use crate::runtime::observability::PipelineSnapshot;

/// Server implementation of MasterService
#[derive(Clone)]
pub struct MasterServiceImpl {
    master: Arc<Master>,
}

impl MasterServiceImpl {
    pub fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            master: Arc::new(Master::new(orchestrator)),
        }
    }

    pub fn master(&self) -> Arc<Master> {
        self.master.clone()
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
        match self
            .master
            .report_checkpoint(checkpoint_id, task, blobs, req.execution_attempt_id)
            .await
        {
            Ok(()) => Ok(Response::new(ReportCheckpointResponse {
                success: true,
                error_message: String::new(),
            })),
            Err(error_message) => Ok(Response::new(ReportCheckpointResponse {
                success: false,
                error_message,
            })),
        }
    }

    async fn report_checkpoint_propagation(
        &self,
        request: Request<ReportCheckpointPropagationRequest>,
    ) -> Result<Response<ReportCheckpointPropagationResponse>, Status> {
        let req = request.into_inner();
        let phase = match ProtoPropagationPhase::try_from(req.phase) {
            Ok(ProtoPropagationPhase::BarrierInjected) => CheckpointPropagationPhase::BarrierInjected,
            Ok(ProtoPropagationPhase::Aligned) => CheckpointPropagationPhase::Aligned,
            Ok(ProtoPropagationPhase::Unspecified) | Err(_) => {
                return Ok(Response::new(ReportCheckpointPropagationResponse {
                    success: false,
                    error_message: format!("invalid checkpoint propagation phase {}", req.phase),
                }));
            }
        };
        let task = TaskKey {
            vertex_id: req.vertex_id,
            task_index: req.task_index,
        };
        match self
            .master
            .report_checkpoint_propagation(
                req.checkpoint_id,
                task,
                req.execution_attempt_id,
                phase,
            )
            .await
        {
            Ok(()) => Ok(Response::new(ReportCheckpointPropagationResponse {
                success: true,
                error_message: String::new(),
            })),
            Err(error_message) => Ok(Response::new(ReportCheckpointPropagationResponse {
                success: false,
                error_message,
            })),
        }
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
        let snapshot_opt: Option<PipelineSnapshot> =
            self.master.get_latest_pipeline_snapshot().await;

        if let Some(snapshot) = snapshot_opt {
            let snapshot_bytes = bincode::serialize(&snapshot).map_err(|e| {
                Status::internal(format!(
                    "Failed to serialize latest pipeline snapshot: {}",
                    e
                ))
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

    async fn get_lifecycle_events(
        &self,
        request: Request<GetLifecycleEventsRequest>,
    ) -> Result<Response<GetLifecycleEventsResponse>, Status> {
        let events = self
            .master
            .lifecycle_events_since(request.into_inner().after_sequence)
            .await
            .into_iter()
            .map(|record| {
                Ok(LifecycleEventRecord {
                    sequence: record.sequence,
                    event_bytes: bincode::serialize(&record.event)
                        .map_err(|error| Status::internal(error.to_string()))?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(GetLifecycleEventsResponse { events }))
    }

    async fn trigger_checkpoint(
        &self,
        _request: Request<TriggerCheckpointRequest>,
    ) -> Result<Response<TriggerCheckpointResponse>, Status> {
        match self.master.force_checkpoint().await {
            Ok(checkpoint_id) => Ok(Response::new(TriggerCheckpointResponse {
                success: true,
                error_message: String::new(),
                checkpoint_id,
            })),
            Err(error_message) => Ok(Response::new(TriggerCheckpointResponse {
                success: false,
                error_message,
                checkpoint_id: 0,
            })),
        }
    }

    async fn stop_sources(
        &self,
        _request: Request<StopSourcesRequest>,
    ) -> Result<Response<StopSourcesResponse>, Status> {
        match self.master.stop_sources().await {
            Ok(()) => Ok(Response::new(StopSourcesResponse {
                success: true,
                error_message: String::new(),
            })),
            Err(error_message) => Ok(Response::new(StopSourcesResponse {
                success: false,
                error_message,
            })),
        }
    }

    async fn get_source_stats(
        &self,
        _request: Request<GetSourceStatsRequest>,
    ) -> Result<Response<GetSourceStatsResponse>, Status> {
        match self.master.get_source_stats().await {
            Ok((tasks, total_records_generated)) => Ok(Response::new(GetSourceStatsResponse {
                success: true,
                error_message: String::new(),
                tasks: tasks
                    .into_iter()
                    .map(|(vertex_id, task_index, records_generated)| SourceTaskStats {
                        vertex_id,
                        task_index,
                        records_generated,
                    })
                    .collect(),
                total_records_generated,
            })),
            Err(error_message) => Ok(Response::new(GetSourceStatsResponse {
                success: false,
                error_message,
                tasks: Vec::new(),
                total_records_generated: 0,
            })),
        }
    }
}
