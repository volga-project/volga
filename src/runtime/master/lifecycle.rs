//! Pipeline lifecycle:
//! schedule → run until finished or failed → recover if failed → repeat.

use std::sync::Arc;

use super::attempt::{AttemptOutcome, ExecutionAttempt, ScheduleError};
use super::state::{MasterState, PipelineContext};

const RECOVERY_BUDGET: u64 = 20;

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
            let mut attempt = ExecutionAttempt::new(
                execution_attempt_id,
                restore_checkpoint_id,
                self.state.clone(),
                pipeline.clone(),
            );
            let schedule_outcome = match attempt.schedule().await {
                Ok(()) => {
                    println!("[MASTER] Scheduling ok attempt={}", execution_attempt_id);
                    attempt.run().await?
                }
                Err(ScheduleError::Terminal(detail)) => {
                    return Err(anyhow::anyhow!("terminal scheduling failure: {}", detail));
                }
                Err(ScheduleError::Recoverable { replace, detail }) => {
                    println!(
                        "[MASTER] Scheduling failed attempt={}: {} replace={:?}",
                        execution_attempt_id, detail, replace
                    );
                    AttemptOutcome::Recover(replace)
                }
            };

            match schedule_outcome {
                AttemptOutcome::Finished => {
                    attempt.finish().await;
                    println!("[MASTER] Pipeline finished");
                    return Ok(());
                }
                AttemptOutcome::Recover(replace) => {
                    if execution_attempt_id >= RECOVERY_BUDGET {
                        return Err(anyhow::anyhow!(
                            "recovery budget exhausted after {} attempts",
                            RECOVERY_BUDGET
                        ));
                    }

                    println!(
                        "[MASTER] Recovering {}/{} after execution attempt {} replace={:?}",
                        execution_attempt_id + 1,
                        RECOVERY_BUDGET,
                        execution_attempt_id,
                        replace
                    );
                    attempt.recover(replace).await?;

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
