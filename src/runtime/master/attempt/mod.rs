use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::error::SendError;
use kameo::message::{Context, Message};
use kameo::reply::{DelegatedReply, ReplySender};
use kameo::Actor;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::failure::FailureEvent;
use crate::orchestrator::orchestrator::WorkerHealthWatchHandle;
use super::state::{MasterState, PipelineContext};
use super::worker_client::WorkerClient;

mod execute;
mod schedule;
mod teardown;

/// Current execution-attempt run phase. Owned by [`ExecutionAttempt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptPhase {
    Running,
    Checkpointing,
    Draining,
}

impl AttemptPhase {
    /// `Checkpointing` → `Running`; leave `Draining` alone.
    fn clear_checkpointing(&mut self) {
        if *self == Self::Checkpointing {
            *self = Self::Running;
        }
    }
}

pub(super) enum AttemptOutcome {
    Finished,
    Recover(HashSet<String>),
}

#[derive(Debug)]
pub(super) enum ScheduleError {
    Terminal(String),
    Recoverable {
        replace: HashSet<String>,
        detail: String,
    },
}

/// Live execution attempt: owns phase and the run loop (via mailbox ticks).
#[derive(Actor)]
pub(super) struct ExecutionAttempt {
    id: u64,
    restore_checkpoint_id: Option<u64>,
    state: Arc<MasterState>,
    pipeline: Arc<PipelineContext>,
    phase: AttemptPhase,
    clients: HashMap<String, WorkerClient>,
    failure_tx: mpsc::Sender<FailureEvent>,
    failure_rx: Option<mpsc::Receiver<FailureEvent>>,
    /// Set while `Run` is in progress; completed by tick/failure handlers.
    run: Option<RunSlot>,
}

struct RunSlot {
    reply: ReplySender<Result<AttemptOutcome, String>>,
    run_loop: JoinHandle<()>,
    _health_poll: Option<WorkerHealthWatchHandle>,
    /// Failures collected during the aggregation window after the first fatal.
    aggregating: Option<Vec<FailureEvent>>,
}

// --- public messages ---

pub(super) struct Schedule;

pub(super) struct Run;

/// StopSources: enter draining and abort any in-flight checkpoint.
pub(super) struct Drain;

pub(super) struct Finish;

pub(super) struct Recover(pub HashSet<String>);

// --- run-loop ticks (run_loop task → self) ---

pub(super) struct PollTick;
pub(super) struct CheckpointTick;
pub(super) struct FailureMsg(pub(super) FailureEvent);
pub(super) struct AggWindowEnd;

impl ExecutionAttempt {
    pub(super) fn spawn(
        id: u64,
        restore_checkpoint_id: Option<u64>,
        state: Arc<MasterState>,
        pipeline: Arc<PipelineContext>,
    ) -> ActorRef<Self> {
        let (failure_tx, failure_rx) = mpsc::channel(256);
        kameo::spawn(Self {
            id,
            restore_checkpoint_id,
            state,
            pipeline,
            phase: AttemptPhase::Running,
            clients: HashMap::new(),
            failure_tx,
            failure_rx: Some(failure_rx),
            run: None,
        })
    }

    fn complete_run(&mut self, outcome: Result<AttemptOutcome, String>) {
        if let Some(slot) = self.run.take() {
            slot.run_loop.abort();
            slot.reply.send(outcome);
        }
    }
}

impl Message<Schedule> for ExecutionAttempt {
    type Reply = Result<(), ScheduleError>;

    async fn handle(
        &mut self,
        _msg: Schedule,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.schedule().await
    }
}

impl Message<Run> for ExecutionAttempt {
    type Reply = DelegatedReply<Result<AttemptOutcome, String>>;

    async fn handle(&mut self, _msg: Run, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (delegated, reply) = ctx.reply_sender();
        let Some(reply) = reply else {
            return delegated;
        };
        if self.run.is_some() {
            reply.send(Err("attempt run already in progress".to_string()));
            return delegated;
        }
        self.phase = AttemptPhase::Running;
        let (run_loop, health_poll) = self.run(ctx.actor_ref());
        self.run = Some(RunSlot {
            reply,
            run_loop,
            _health_poll: health_poll,
            aggregating: None,
        });
        delegated
    }
}

impl Message<Drain> for ExecutionAttempt {
    type Reply = Result<Option<u64>, String>;

    async fn handle(
        &mut self,
        _msg: Drain,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.phase = AttemptPhase::Draining;
        let checkpoint_id = self
            .state
            .abort_in_flight_checkpoint(
                self.id,
                "aborted: sources stopped for drain".to_string(),
            )
            .await?;
        if let Some(checkpoint_id) = checkpoint_id {
            println!(
                "[MASTER] Checkpoint {} aborted: sources stopped for drain attempt={}",
                checkpoint_id, self.id
            );
        }
        Ok(checkpoint_id)
    }
}

impl Message<Finish> for ExecutionAttempt {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: Finish,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.finish().await;
    }
}

impl Message<Recover> for ExecutionAttempt {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: Recover,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.recover(msg.0)
            .await
            .map_err(|error| error.to_string())
    }
}

impl Message<PollTick> for ExecutionAttempt {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: PollTick,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.on_poll_tick().await;
    }
}

impl Message<CheckpointTick> for ExecutionAttempt {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: CheckpointTick,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.on_checkpoint_tick().await;
    }
}

impl Message<FailureMsg> for ExecutionAttempt {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: FailureMsg,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.on_failure(msg.0, ctx.actor_ref()).await;
    }
}

impl Message<AggWindowEnd> for ExecutionAttempt {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: AggWindowEnd,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.on_agg_window_end().await;
    }
}

/// Map a schedule `ask` into the domain `ScheduleError` (or a transport failure).
pub(super) fn schedule_ask_error(
    error: SendError<Schedule, ScheduleError>,
) -> ScheduleError {
    match error {
        SendError::HandlerError(error) => error,
        other => ScheduleError::Terminal(format!("{other:?}")),
    }
}
