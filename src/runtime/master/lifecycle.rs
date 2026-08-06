//! Pipeline lifecycle actor: pipeline intent + attempt supervision.
//!
//! `RequestFinish` sets intent=`Finish` and drains only when the current attempt
//! is stably runnable. Recovering attempts are left alone; drain is retried after
//! the next attempt reaches Running.

use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::reply::{DelegatedReply, ReplySender};
use kameo::Actor;

use super::attempt::{
    schedule_ask_error, AttemptOutcome, Drain, ExecutionAttempt, Finish, Recover, Run, Schedule,
    ScheduleError,
};
use super::events::LifecycleEvent;
use super::state::{MasterState, PipelineContext};
use crate::runtime::consts::{runtime_consts, MASTER_RECOVERY_BUDGET};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelineIntent {
    Run,
    Finish,
}

#[derive(Actor)]
pub(super) struct MasterLifecycle {
    state: Arc<MasterState>,
    intent: PipelineIntent,
    pipeline: Option<Arc<PipelineContext>>,
    execute_reply: Option<ReplySender<Result<(), String>>>,
    attempt_id: u64,
    restore_checkpoint_id: Option<u64>,
    current: Option<ActorRef<ExecutionAttempt>>,
}

pub(super) struct Start(pub PipelineContext);

/// Cooperative pipeline finish: set intent and drain when the attempt allows it.
pub(super) struct RequestFinish;

struct RunFinished(Result<AttemptOutcome, String>);

/// Attempt run loop has started; safe to `Drain` if intent is `Finish`.
pub(super) struct RunStarted;

impl MasterLifecycle {
    pub(super) fn spawn(state: Arc<MasterState>) -> ActorRef<Self> {
        kameo::spawn(Self {
            state,
            intent: PipelineIntent::Run,
            pipeline: None,
            execute_reply: None,
            attempt_id: 0,
            restore_checkpoint_id: None,
            current: None,
        })
    }

    fn complete_execute(&mut self, result: Result<(), String>) {
        if let Some(reply) = self.execute_reply.take() {
            reply.send(result);
        }
        self.pipeline = None;
        self.current = None;
        self.intent = PipelineIntent::Run;
    }

    async fn try_drain(&self) {
        let Some(attempt) = self.current.as_ref() else {
            return;
        };
        match attempt.ask(Drain).await {
            Ok(()) => {
                println!(
                    "[MASTER] StopSources ok execution_attempt={}",
                    self.attempt_id
                );
            }
            Err(error) => {
                println!(
                    "[MASTER] Drain deferred attempt={}: {error}",
                    self.attempt_id
                );
            }
        }
    }

    async fn start_attempt(&mut self, self_ref: ActorRef<Self>) -> Result<(), String> {
        let pipeline = self
            .pipeline
            .clone()
            .ok_or_else(|| "lifecycle has no pipeline".to_string())?;

        self.state
            .record_lifecycle_event(LifecycleEvent::AttemptStarted {
                attempt_id: self.attempt_id,
                restore_checkpoint_id: self.restore_checkpoint_id,
            })
            .await;
        self.state.set_current_attempt_id(self.attempt_id);
        let attempt = ExecutionAttempt::spawn(
            self.attempt_id,
            self.restore_checkpoint_id,
            self.state.clone(),
            pipeline,
            self_ref.clone(),
        );
        self.state.set_current_attempt(attempt.clone()).await;
        self.current = Some(attempt.clone());

        match attempt.ask(Schedule).await {
            Ok(()) => {
                println!("[MASTER] Scheduling ok attempt={}", self.attempt_id);
                let lifecycle = self_ref.clone();
                let run_attempt = attempt.clone();
                tokio::spawn(async move {
                    let result = match run_attempt.ask(Run).await {
                        Ok(outcome) => Ok(outcome),
                        Err(error) => Err(error.to_string()),
                    };
                    let _ = lifecycle.tell(RunFinished(result)).await;
                });
                // Drain runs on RunStarted (after Run starts the poll loop).
                Ok(())
            }
            Err(error) => match schedule_ask_error(error) {
                ScheduleError::Terminal(detail) => {
                    self.state.clear_current_attempt().await;
                    self.state
                        .record_lifecycle_event(LifecycleEvent::PipelineFailed {
                            detail: detail.clone(),
                        })
                        .await;
                    let _ = attempt.stop_gracefully().await;
                    self.current = None;
                    Err(format!("terminal scheduling failure: {detail}"))
                }
                ScheduleError::Recoverable { replace, detail } => {
                    println!(
                        "[MASTER] Scheduling failed attempt={}: {} replace={:?}",
                        self.attempt_id, detail, replace
                    );
                    // Bounce through the mailbox to avoid async recursion with handle_outcome.
                    let _ = self_ref
                        .tell(RunFinished(Ok(AttemptOutcome::Recover(replace))))
                        .await;
                    Ok(())
                }
            },
        }
    }

    async fn handle_outcome(
        &mut self,
        result: Result<AttemptOutcome, String>,
        self_ref: ActorRef<Self>,
    ) -> Result<(), String> {
        let Some(attempt) = self.current.take() else {
            return Err("no current attempt for outcome".to_string());
        };

        match result {
            Ok(AttemptOutcome::Finished) => {
                if let Err(error) = attempt.ask(Finish).await {
                    self.state.clear_current_attempt().await;
                    let _ = attempt.stop_gracefully().await;
                    return Err(format!("finish failed: {error}"));
                }
                self.state.clear_current_attempt().await;
                let _ = attempt.stop_gracefully().await;
                self.state
                    .record_lifecycle_event(LifecycleEvent::PipelineFinished)
                    .await;
                println!("[MASTER] Pipeline finished");
                Ok(())
            }
            Ok(AttemptOutcome::Recover(replace)) => {
                if self.attempt_id >= runtime_consts().u64(MASTER_RECOVERY_BUDGET) {
                    self.state.clear_current_attempt().await;
                    let _ = attempt.stop_gracefully().await;
                    return Err(format!(
                        "recovery budget exhausted after {} attempts",
                        runtime_consts().u64(MASTER_RECOVERY_BUDGET)
                    ));
                }

                let replacement_worker_ids = replace.iter().cloned().collect();
                self.state
                    .record_lifecycle_event(LifecycleEvent::RecoveryStarted {
                        attempt_id: self.attempt_id,
                        replacement_worker_ids,
                    })
                    .await;
                println!(
                    "[MASTER] Recovering {}/{} after execution attempt {} replace={:?}",
                    self.attempt_id + 1,
                    runtime_consts().u64(MASTER_RECOVERY_BUDGET),
                    self.attempt_id,
                    replace
                );
                if let Err(error) = attempt.ask(Recover(replace)).await {
                    self.state.clear_current_attempt().await;
                    let _ = attempt.stop_gracefully().await;
                    return Err(error.to_string());
                }
                self.state.clear_current_attempt().await;
                let _ = attempt.stop_gracefully().await;

                self.attempt_id += 1;
                self.restore_checkpoint_id = self.state.latest_complete_checkpoint().await;
                println!(
                    "[MASTER] Starting execution attempt {} restore={:?}",
                    self.attempt_id, self.restore_checkpoint_id
                );
                self.start_attempt(self_ref).await
            }
            Err(detail) => {
                self.state.clear_current_attempt().await;
                self.state
                    .record_lifecycle_event(LifecycleEvent::PipelineFailed {
                        detail: detail.clone(),
                    })
                    .await;
                let _ = attempt.stop_gracefully().await;
                Err(detail)
            }
        }
    }
}

impl Message<Start> for MasterLifecycle {
    type Reply = DelegatedReply<Result<(), String>>;

    async fn handle(&mut self, msg: Start, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (delegated, reply) = ctx.reply_sender();
        let Some(reply) = reply else {
            return delegated;
        };
        if self.execute_reply.is_some() {
            reply.send(Err("lifecycle already running".to_string()));
            return delegated;
        }

        println!("[MASTER] Starting pipeline lifecycle");
        self.intent = PipelineIntent::Run;
        self.pipeline = Some(Arc::new(msg.0));
        self.execute_reply = Some(reply);
        self.attempt_id = 0;
        self.restore_checkpoint_id = None;
        self.current = None;

        match self.start_attempt(ctx.actor_ref()).await {
            Ok(()) => {}
            Err(error) => self.complete_execute(Err(error)),
        }
        delegated
    }
}

impl Message<RequestFinish> for MasterLifecycle {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        _msg: RequestFinish,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.execute_reply.is_none() {
            return Err("lifecycle not started".to_string());
        }
        self.intent = PipelineIntent::Finish;
        self.try_drain().await;
        Ok(())
    }
}

impl Message<RunFinished> for MasterLifecycle {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RunFinished,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.execute_reply.is_none() {
            return;
        }
        match self.handle_outcome(msg.0, ctx.actor_ref()).await {
            Ok(()) => {
                // Recover path may have started another attempt (execute_reply still set).
                if self.current.is_none() {
                    self.complete_execute(Ok(()));
                }
            }
            Err(error) => self.complete_execute(Err(error)),
        }
    }
}

impl Message<RunStarted> for MasterLifecycle {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: RunStarted,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.intent == PipelineIntent::Finish {
            self.try_drain().await;
        }
    }
}
