use std::time::Duration;
use std::collections::HashSet;

use anyhow::{anyhow, Result};
use arrow::array::StringArray;

use super::handles::MasterHandle;
use crate::runtime::master::{LifecycleEvent, LifecycleEventRecord};
use crate::storage::InMemoryStorageSnapshot;

#[derive(Debug, Clone, Default)]
pub struct OutputOracle;

impl OutputOracle {
    pub fn assert_string_column_values(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
        expected_values: &[String],
        expected_total_rows: usize,
    ) -> Result<()> {
        let values = Self::string_column_values(snapshot, column_index)?;
        if values.len() != expected_total_rows {
            return Err(anyhow!(
                "expected {expected_total_rows} output rows, got {}",
                values.len()
            ));
        }
        Self::assert_expected_values(&values, expected_values)
    }

    fn assert_expected_values(values: &[String], expected_values: &[String]) -> Result<()> {
        let expected = expected_values
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for expected_value in &expected {
            if !values.iter().any(|value| value == expected_value) {
                return Err(anyhow!(
                    "expected output value '{expected_value}' was not emitted"
                ));
            }
        }
        for value in values {
            if !expected.contains(value.as_str()) {
                return Err(anyhow!("unexpected output value '{value}'"));
            }
        }
        Ok(())
    }

    pub fn assert_string_column_matches(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
        expected_total_rows: usize,
        description: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> Result<()> {
        let values = Self::string_column_values(snapshot, column_index)?;
        if values.len() != expected_total_rows {
            return Err(anyhow!(
                "expected {expected_total_rows} output rows, got {}",
                values.len()
            ));
        }
        for value in values {
            if !predicate(&value) {
                return Err(anyhow!(
                    "output value '{value}' does not satisfy {description}"
                ));
            }
        }
        Ok(())
    }

    fn string_column_values(
        snapshot: &InMemoryStorageSnapshot,
        column_index: usize,
    ) -> Result<Vec<String>> {
        let mut output = Vec::new();
        for batch in snapshot.record_batches() {
            let values = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray at output column {column_index}"))?;
            output.extend((0..batch.num_rows()).map(|row| values.value(row).to_string()));
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LifecycleOracle;

impl LifecycleOracle {
    pub async fn wait_for(
        master: &MasterHandle,
        cursor: &mut u64,
        timeout: Duration,
        predicate: impl Fn(&LifecycleEvent) -> bool,
    ) -> Result<LifecycleEventRecord> {
        let started = tokio::time::Instant::now();
        loop {
            for record in master.lifecycle_events_since(*cursor).await? {
                *cursor = record.sequence;
                if predicate(&record.event) {
                    return Ok(record);
                }
            }
            if started.elapsed() >= timeout {
                return Err(anyhow!("timed out waiting for lifecycle event"));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Print FT summary grouped by attempt (trigger, per-worker events, replaced/reused).
    pub fn print_ft_stats(events: &[LifecycleEventRecord]) {
        #[derive(Default)]
        struct Attempt {
            trigger: String,
            workers: Vec<String>,
            lines: Vec<String>,
            replaced: Vec<String>,
            reused: Vec<String>,
            recovered: bool,
        }

        let mut by_attempt: std::collections::BTreeMap<u64, Attempt> = Default::default();
        let mut current: Option<u64> = None;
        let mut pending_recovery: Option<u64> = None;
        let mut outcome = String::new();

        for record in events {
            match &record.event {
                LifecycleEvent::AttemptStarted {
                    attempt_id,
                    restore_checkpoint_id,
                } => {
                    current = Some(*attempt_id);
                    let trigger = if *attempt_id == 0 {
                        "initial".to_string()
                    } else {
                        let prev = attempt_id - 1;
                        let prev_failures: Vec<String> = by_attempt
                            .get(&prev)
                            .map(|a| {
                                a.lines
                                    .iter()
                                    .filter(|l| l.starts_with("failure "))
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();
                        if prev_failures.is_empty() {
                            format!(
                                "recovery from attempt {prev} (scheduling/other) restore={restore_checkpoint_id:?}"
                            )
                        } else {
                            format!(
                                "recovery from attempt {prev} restore={restore_checkpoint_id:?}; prior: {}",
                                prev_failures.join("; ")
                            )
                        }
                    };
                    by_attempt.entry(*attempt_id).or_default().trigger = trigger;
                }
                LifecycleEvent::AttemptScheduled { attempt_id, worker_ids }
                | LifecycleEvent::AttemptRunning { attempt_id, worker_ids } => {
                    current = Some(*attempt_id);
                    let entry = by_attempt.entry(*attempt_id).or_default();
                    entry.workers = worker_ids.clone();
                    entry.lines.push(format!(
                        "{} workers={worker_ids:?}",
                        match &record.event {
                            LifecycleEvent::AttemptScheduled { .. } => "scheduled",
                            _ => "running",
                        }
                    ));
                }
                LifecycleEvent::WorkerFailure {
                    attempt_id,
                    worker_id,
                    kind,
                    detail,
                } => {
                    current = Some(*attempt_id);
                    by_attempt.entry(*attempt_id).or_default().lines.push(format!(
                        "failure worker={worker_id} kind={kind} detail={detail}"
                    ));
                }
                LifecycleEvent::RecoveryStarted {
                    attempt_id,
                    replacement_worker_ids,
                } => {
                    pending_recovery = Some(*attempt_id);
                    by_attempt.entry(*attempt_id).or_default().lines.push(format!(
                        "recovery_started initial_replace={replacement_worker_ids:?}"
                    ));
                }
                LifecycleEvent::ReplacementRequested { worker_ids } => {
                    let attempt_id = pending_recovery
                        .take()
                        .or(current)
                        .unwrap_or(0);
                    let entry = by_attempt.entry(attempt_id).or_default();
                    let reused: Vec<_> = entry
                        .workers
                        .iter()
                        .filter(|id| !worker_ids.contains(id))
                        .cloned()
                        .collect();
                    entry.replaced = worker_ids.clone();
                    entry.reused = reused.clone();
                    entry.recovered = true;
                    entry.lines.push(format!(
                        "replacement_requested replaced={worker_ids:?} reused={reused:?}"
                    ));
                }
                LifecycleEvent::WorkerRegistered { worker_id } => {
                    if let Some(attempt_id) = current {
                        by_attempt.entry(attempt_id).or_default().lines.push(format!(
                            "worker_registered {worker_id}"
                        ));
                    }
                }
                LifecycleEvent::PipelineFinished => {
                    outcome = "finished".to_string();
                }
                LifecycleEvent::PipelineFailed { detail } => {
                    outcome = format!("failed: {detail}");
                }
                LifecycleEvent::CheckpointCompleted { checkpoint_id } => {
                    if let Some(attempt_id) = current {
                        by_attempt.entry(attempt_id).or_default().lines.push(format!(
                            "checkpoint_completed {checkpoint_id}"
                        ));
                    }
                }
            }
        }

        println!("[FT] ---- fault tolerance summary ----");
        println!("[FT] attempts={}", by_attempt.len());
        for (attempt_id, attempt) in &by_attempt {
            println!("[FT] attempt {attempt_id}");
            println!("[FT]   trigger: {}", attempt.trigger);
            println!("[FT]   workers: {:?}", attempt.workers);
            if attempt.lines.is_empty() {
                println!("[FT]   events: (none)");
            } else {
                println!("[FT]   events:");
                for line in &attempt.lines {
                    println!("[FT]     - {line}");
                }
            }
            if attempt.recovered {
                println!("[FT]   replaced: {:?}", attempt.replaced);
                println!("[FT]   reused:   {:?}", attempt.reused);
            }
        }
        if !outcome.is_empty() {
            println!("[FT] pipeline outcome: {outcome}");
        }
        println!("[FT] --------------------------------");
    }
}
