//! Owns checkpoint protocol + store IO on a single mailbox.
//!
//! Persist runs inside the actor turn (no `PendingPersist` / lock escape hatch).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use kameo::message::{Context, Message};
use kameo::Actor;

use crate::common::types::PipelineId;
use crate::runtime::checkpoint::{CompletedCheckpoint, SerializedCheckpoint};

use super::store::CheckpointStore;
use super::{CheckpointAckOutcome, CheckpointStartError, Checkpoints, TaskKey};

#[derive(Actor, Debug, Default)]
pub struct CheckpointCoordinator {
    inner: Checkpoints,
}

/// Configure expected tasks and store for a pipeline.
pub struct ConfigureCheckpoints {
    pub pipeline_id: PipelineId,
    pub expected_acks: HashSet<TaskKey>,
    pub expected_aligns: HashSet<TaskKey>,
    pub retention: usize,
    pub store: Arc<dyn CheckpointStore>,
}

pub struct StartCheckpoint;

pub struct AbortInFlightCheckpoint;

pub struct ReportCheckpoint {
    pub checkpoint_id: u64,
    pub task: TaskKey,
    pub checkpoint: SerializedCheckpoint,
}

/// Barrier Injected/Aligned. `None` reply = not in-flight (soft ignore).
pub struct NoteBarrierProgress {
    pub checkpoint_id: u64,
    pub task: TaskKey,
}

pub struct InFlightCheckpointId;

pub struct InFlightCheckpointTimedOut(pub Duration);

pub struct LatestCompleteCheckpoint;

pub struct LoadCheckpoint(pub u64);

impl Message<ConfigureCheckpoints> for CheckpointCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ConfigureCheckpoints,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.configure(
            msg.pipeline_id,
            msg.expected_acks,
            msg.expected_aligns,
            msg.retention,
            msg.store,
        );
    }
}

impl Message<StartCheckpoint> for CheckpointCoordinator {
    type Reply = Result<u64, CheckpointStartError>;

    async fn handle(
        &mut self,
        _msg: StartCheckpoint,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.start()
    }
}

impl Message<AbortInFlightCheckpoint> for CheckpointCoordinator {
    type Reply = Option<(u64, u64)>;

    async fn handle(
        &mut self,
        _msg: AbortInFlightCheckpoint,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.abort_in_flight()
    }
}

impl Message<ReportCheckpoint> for CheckpointCoordinator {
    type Reply = Result<CheckpointAckOutcome>;

    async fn handle(
        &mut self,
        msg: ReportCheckpoint,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner
            .report(msg.checkpoint_id, msg.task, msg.checkpoint)
            .await
    }
}

impl Message<NoteBarrierProgress> for CheckpointCoordinator {
    type Reply = Result<Option<CheckpointAckOutcome>>;

    async fn handle(
        &mut self,
        msg: NoteBarrierProgress,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.inner.in_flight_id() != Some(msg.checkpoint_id) {
            return Ok(None);
        }
        self.inner
            .note_barrier_progress(msg.checkpoint_id, msg.task)
            .await
            .map(Some)
    }
}

impl Message<InFlightCheckpointId> for CheckpointCoordinator {
    type Reply = Option<u64>;

    async fn handle(
        &mut self,
        _msg: InFlightCheckpointId,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.in_flight_id()
    }
}

impl Message<InFlightCheckpointTimedOut> for CheckpointCoordinator {
    type Reply = Option<u64>;

    async fn handle(
        &mut self,
        msg: InFlightCheckpointTimedOut,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.in_flight_timed_out(msg.0)
    }
}

impl Message<LatestCompleteCheckpoint> for CheckpointCoordinator {
    type Reply = Option<u64>;

    async fn handle(
        &mut self,
        _msg: LatestCompleteCheckpoint,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.latest_complete()
    }
}

impl Message<LoadCheckpoint> for CheckpointCoordinator {
    type Reply = Result<CompletedCheckpoint>;

    async fn handle(
        &mut self,
        msg: LoadCheckpoint,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.inner.load(msg.0).await
    }
}
