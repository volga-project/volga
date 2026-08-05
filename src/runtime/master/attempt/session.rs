//! One master-side control session per worker for an execution attempt.

use kameo::message::{Context, Message};
use kameo::Actor;
use tokio::sync::mpsc;

use crate::api::PipelineSpec;
use crate::common::failure::FailureEvent;
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::checkpoint::{SerializedRestore, TaskKey};
use crate::runtime::observability::snapshot_types::WorkerSnapshot;

use super::super::worker_client::{WorkerCallError, WorkerClient};

#[derive(Actor)]
pub(super) struct WorkerSession {
    worker_id: String,
    client: WorkerClient,
}

impl WorkerSession {
    pub(super) fn spawn(worker_id: String, client: WorkerClient) -> kameo::actor::ActorRef<Self> {
        kameo::spawn(Self { worker_id, client })
    }
}

pub(super) struct StartHeartbeat {
    pub failure_tx: mpsc::Sender<FailureEvent>,
}

pub(super) struct Configure {
    pub pipeline_id: String,
    pub spec: PipelineSpec,
    pub vertex_ids: Vec<String>,
    pub mapping: TaskWorkerMapping,
    pub task_restore_data: Vec<(TaskKey, SerializedRestore)>,
    pub restoring: bool,
}

pub(super) struct StartWorker;
pub(super) struct RunTasks;
pub(super) struct GetWorkerState;
pub(super) struct TriggerBarrier(pub u64);
pub(super) struct StopSources;
pub(super) struct ResetWorker;
pub(super) struct CloseTasks;
pub(super) struct ShutdownWorker;

impl Message<StartHeartbeat> for WorkerSession {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StartHeartbeat,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .start_heartbeat(self.worker_id.clone(), msg.failure_tx);
    }
}

impl Message<Configure> for WorkerSession {
    type Reply = Result<String, WorkerCallError>;

    async fn handle(
        &mut self,
        msg: Configure,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .configure(
                self.worker_id.clone(),
                msg.pipeline_id,
                msg.spec,
                msg.vertex_ids,
                msg.mapping,
                msg.task_restore_data,
                msg.restoring,
            )
            .await
    }
}

impl Message<StartWorker> for WorkerSession {
    type Reply = Result<(), WorkerCallError>;

    async fn handle(
        &mut self,
        _msg: StartWorker,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client.start_worker().await
    }
}

impl Message<RunTasks> for WorkerSession {
    type Reply = Result<(), WorkerCallError>;

    async fn handle(
        &mut self,
        _msg: RunTasks,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client.run_worker_tasks().await
    }
}

impl Message<GetWorkerState> for WorkerSession {
    type Reply = Result<WorkerSnapshot, WorkerCallError>;

    async fn handle(
        &mut self,
        _msg: GetWorkerState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client.get_worker_state().await
    }
}

impl Message<TriggerBarrier> for WorkerSession {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        msg: TriggerBarrier,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .trigger_checkpoint_barrier(msg.0)
            .await
            .map_err(|error| error.to_string())
    }
}

impl Message<StopSources> for WorkerSession {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: StopSources,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .stop_sources()
            .await
            .map_err(|error| error.to_string())
    }
}

impl Message<ResetWorker> for WorkerSession {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: ResetWorker,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .reset_worker()
            .await
            .map_err(|error| error.to_string())
    }
}

impl Message<CloseTasks> for WorkerSession {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: CloseTasks,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .close_worker_tasks()
            .await
            .map_err(|error| error.to_string())
    }
}

impl Message<ShutdownWorker> for WorkerSession {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: ShutdownWorker,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client
            .shutdown_worker()
            .await
            .map_err(|error| error.to_string())
    }
}

pub(super) fn map_session_error<M>(
    error: kameo::error::SendError<M, WorkerCallError>,
) -> WorkerCallError {
    match error {
        kameo::error::SendError::HandlerError(error) => error,
        other => WorkerCallError::Unreachable(format!("{other:?}")),
    }
}
