//! Pipeline lifecycle:
//! schedule → run until finished or failed → recover if failed → repeat.

use std::sync::Arc;

use super::attempt::{
    schedule_ask_error, AttemptOutcome, ExecutionAttempt, Finish, Recover, Run, Schedule,
    ScheduleError,
};
use super::events::LifecycleEvent;
use super::state::{MasterState, PipelineContext};
use crate::runtime::consts::{runtime_consts, MASTER_RECOVERY_BUDGET};

pub(super) struct MasterLifecycle {
    state: Arc<MasterState>,
}

impl MasterLifecycle {
    pub(super) fn new(state: Arc<MasterState>) -> Self {
        Self { state }
    }

    pub(super) async fn run(&self, pipeline: PipelineContext) -> anyhow::Result<()> {
        println!("[MASTER] Starting pipeline lifecycle");
        let mut execution_attempt_id = 0;
        let mut restore_checkpoint_id = None;
        let pipeline = Arc::new(pipeline);

        loop {
            self.state
                .record_lifecycle_event(LifecycleEvent::AttemptStarted {
                    attempt_id: execution_attempt_id,
                    restore_checkpoint_id,
                })
                .await;
            self.state.set_current_attempt_id(execution_attempt_id);
            let attempt = ExecutionAttempt::spawn(
                execution_attempt_id,
                restore_checkpoint_id,
                self.state.clone(),
                pipeline.clone(),
            );
            self.state.set_current_attempt(attempt.clone());

            let schedule_outcome = match attempt.ask(Schedule).await {
                Ok(()) => {
                    println!("[MASTER] Scheduling ok attempt={}", execution_attempt_id);
                    match attempt.ask(Run).await {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            self.state.clear_current_attempt();
                            let detail = error.to_string();
                            self.state
                                .record_lifecycle_event(LifecycleEvent::PipelineFailed {
                                    detail: detail.clone(),
                                })
                                .await;
                            let _ = attempt.stop_gracefully().await;
                            return Err(anyhow::anyhow!(detail));
                        }
                    }
                }
                Err(error) => match schedule_ask_error(error) {
                    ScheduleError::Terminal(detail) => {
                        self.state.clear_current_attempt();
                        self.state
                            .record_lifecycle_event(LifecycleEvent::PipelineFailed {
                                detail: detail.clone(),
                            })
                            .await;
                        let _ = attempt.stop_gracefully().await;
                        return Err(anyhow::anyhow!("terminal scheduling failure: {}", detail));
                    }
                    ScheduleError::Recoverable { replace, detail } => {
                        println!(
                            "[MASTER] Scheduling failed attempt={}: {} replace={:?}",
                            execution_attempt_id, detail, replace
                        );
                        AttemptOutcome::Recover(replace)
                    }
                },
            };

            match schedule_outcome {
                AttemptOutcome::Finished => {
                    if let Err(error) = attempt.ask(Finish).await {
                        self.state.clear_current_attempt();
                        let _ = attempt.stop_gracefully().await;
                        return Err(anyhow::anyhow!("finish failed: {error}"));
                    }
                    self.state.clear_current_attempt();
                    let _ = attempt.stop_gracefully().await;
                    self.state
                        .record_lifecycle_event(LifecycleEvent::PipelineFinished)
                        .await;
                    println!("[MASTER] Pipeline finished");
                    return Ok(());
                }
                AttemptOutcome::Recover(replace) => {
                    if execution_attempt_id >= runtime_consts().u64(MASTER_RECOVERY_BUDGET) {
                        self.state.clear_current_attempt();
                        let _ = attempt.stop_gracefully().await;
                        return Err(anyhow::anyhow!(
                            "recovery budget exhausted after {} attempts",
                            runtime_consts().u64(MASTER_RECOVERY_BUDGET)
                        ));
                    }

                    let replacement_worker_ids = replace.iter().cloned().collect();
                    self.state
                        .record_lifecycle_event(LifecycleEvent::RecoveryStarted {
                            attempt_id: execution_attempt_id,
                            replacement_worker_ids,
                        })
                        .await;
                    println!(
                        "[MASTER] Recovering {}/{} after execution attempt {} replace={:?}",
                        execution_attempt_id + 1,
                        runtime_consts().u64(MASTER_RECOVERY_BUDGET),
                        execution_attempt_id,
                        replace
                    );
                    if let Err(error) = attempt.ask(Recover(replace)).await {
                        self.state.clear_current_attempt();
                        let _ = attempt.stop_gracefully().await;
                        return Err(anyhow::anyhow!(error));
                    }
                    self.state.clear_current_attempt();
                    let _ = attempt.stop_gracefully().await;

                    execution_attempt_id += 1;
                    restore_checkpoint_id = self.state.latest_complete_checkpoint().await;
                    println!(
                        "[MASTER] Starting execution attempt {} restore={:?}",
                        execution_attempt_id, restore_checkpoint_id
                    );
                }
            }
        }
    }
}
