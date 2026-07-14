use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

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

    pub fn print_recovery_stats(events: &[LifecycleEventRecord]) {
        RecoveryReport::from_events(events).print();
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryFailure {
    pub worker_id: String,
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryAttemptReport {
    pub attempt_id: u64,
    pub trigger: String,
    pub workers: Vec<String>,
    pub failures: Vec<RecoveryFailure>,
    pub initial_replace: Vec<String>,
    pub replaced: Vec<String>,
    pub reused: Vec<String>,
    pub recovered: bool,
    pub events: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub attempts: BTreeMap<u64, RecoveryAttemptReport>,
    pub outcome: String,
}

impl RecoveryReport {
    pub fn from_events(events: &[LifecycleEventRecord]) -> Self {
        let mut by_attempt: BTreeMap<u64, RecoveryAttemptReport> = BTreeMap::new();
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
                                a.failures
                                    .iter()
                                    .map(|f| {
                                        format!(
                                            "failure worker={} kind={} detail={}",
                                            f.worker_id, f.kind, f.detail
                                        )
                                    })
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
                    attempt_entry(&mut by_attempt, *attempt_id).trigger = trigger;
                }
                LifecycleEvent::AttemptScheduled { attempt_id, worker_ids }
                | LifecycleEvent::AttemptRunning { attempt_id, worker_ids } => {
                    current = Some(*attempt_id);
                    let entry = attempt_entry(&mut by_attempt, *attempt_id);
                    entry.workers = worker_ids.clone();
                    entry.events.push(format!(
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
                    let entry = attempt_entry(&mut by_attempt, *attempt_id);
                    entry.failures.push(RecoveryFailure {
                        worker_id: worker_id.clone(),
                        kind: kind.clone(),
                        detail: detail.clone(),
                    });
                    entry.events.push(format!(
                        "failure worker={worker_id} kind={kind} detail={detail}"
                    ));
                }
                LifecycleEvent::RecoveryStarted {
                    attempt_id,
                    replacement_worker_ids,
                } => {
                    pending_recovery = Some(*attempt_id);
                    let entry = attempt_entry(&mut by_attempt, *attempt_id);
                    entry.initial_replace = replacement_worker_ids.clone();
                    entry.events.push(format!(
                        "recovery_started initial_replace={replacement_worker_ids:?}"
                    ));
                }
                LifecycleEvent::ReplacementRequested { worker_ids } => {
                    let attempt_id = pending_recovery.take().or(current).unwrap_or(0);
                    let entry = attempt_entry(&mut by_attempt, attempt_id);
                    let reused: Vec<_> = entry
                        .workers
                        .iter()
                        .filter(|id| !worker_ids.contains(id))
                        .cloned()
                        .collect();
                    entry.replaced = worker_ids.clone();
                    entry.reused = reused.clone();
                    entry.recovered = true;
                    entry.events.push(format!(
                        "replacement_requested replaced={worker_ids:?} reused={reused:?}"
                    ));
                }
                LifecycleEvent::WorkerRegistered { worker_id } => {
                    if let Some(attempt_id) = current {
                        attempt_entry(&mut by_attempt, attempt_id)
                            .events
                            .push(format!("worker_registered {worker_id}"));
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
                        attempt_entry(&mut by_attempt, attempt_id)
                            .events
                            .push(format!("checkpoint_completed {checkpoint_id}"));
                    }
                }
            }
        }

        Self {
            attempts: by_attempt,
            outcome,
        }
    }

    pub fn print(&self) {
        println!("[RECOVERY] ---- recovery summary ----");
        println!("[RECOVERY] attempts={}", self.attempts.len());
        for (attempt_id, attempt) in &self.attempts {
            println!("[RECOVERY] attempt {attempt_id}");
            println!("[RECOVERY]   trigger: {}", attempt.trigger);
            println!("[RECOVERY]   workers: {:?}", attempt.workers);
            if attempt.events.is_empty() {
                println!("[RECOVERY]   events: (none)");
            } else {
                println!("[RECOVERY]   events:");
                for line in &attempt.events {
                    println!("[RECOVERY]     - {line}");
                }
            }
            println!(
                "[RECOVERY]   initial_replace: {:?}",
                attempt.initial_replace
            );
            if attempt.recovered {
                println!("[RECOVERY]   replaced: {:?}", attempt.replaced);
                println!("[RECOVERY]   reused:   {:?}", attempt.reused);
            }
        }
        if !self.outcome.is_empty() {
            println!("[RECOVERY] pipeline outcome: {}", self.outcome);
        }
        println!("[RECOVERY] --------------------------------");
    }

    pub fn assert_attempt_count(&self, expected: usize) -> Result<()> {
        if self.attempts.len() != expected {
            return Err(anyhow!(
                "expected {expected} attempts, got {}",
                self.attempts.len()
            ));
        }
        Ok(())
    }

    pub fn attempt(&self, attempt_id: u64) -> Result<&RecoveryAttemptReport> {
        self.attempts
            .get(&attempt_id)
            .ok_or_else(|| anyhow!("missing attempt {attempt_id}"))
    }
}

impl RecoveryAttemptReport {
    pub fn assert_has_failure(&self, worker_id: &str, kind: &str) -> Result<()> {
        if self
            .failures
            .iter()
            .any(|f| f.worker_id == worker_id && f.kind == kind)
        {
            return Ok(());
        }
        Err(anyhow!(
            "attempt {}: expected failure worker={worker_id} kind={kind}, got {:?}",
            self.attempt_id,
            self.failures
                .iter()
                .map(|f| format!("{}:{}", f.worker_id, f.kind))
                .collect::<Vec<_>>()
        ))
    }

    /// Assert exactly `expected` distinct workers have a failure of `kind`.
    pub fn assert_failure_kind_distinct_workers(
        &self,
        kind: &str,
        expected: usize,
    ) -> Result<()> {
        let workers: HashSet<_> = self
            .failures
            .iter()
            .filter(|f| f.kind == kind)
            .map(|f| f.worker_id.as_str())
            .collect();
        if workers.len() == expected {
            return Ok(());
        }
        Err(anyhow!(
            "attempt {}: expected {expected} distinct workers with kind={kind}, got {} ({:?})",
            self.attempt_id,
            workers.len(),
            workers
        ))
    }

    pub fn assert_initial_replace_eq(&self, expected: &[&str]) -> Result<()> {
        assert_sorted_ids_eq(
            self.attempt_id,
            "initial_replace",
            &self.initial_replace,
            expected,
        )
    }

    pub fn assert_replaced_eq(&self, expected: &[&str]) -> Result<()> {
        assert_sorted_ids_eq(self.attempt_id, "replaced", &self.replaced, expected)
    }

    pub fn assert_reused_eq(&self, expected: &[&str]) -> Result<()> {
        assert_sorted_ids_eq(self.attempt_id, "reused", &self.reused, expected)
    }

    pub fn assert_initial_replace_contains(&self, worker_id: &str) -> Result<()> {
        if self.initial_replace.iter().any(|id| id == worker_id) {
            return Ok(());
        }
        Err(anyhow!(
            "attempt {}: expected initial_replace to contain {worker_id}, got {:?}",
            self.attempt_id,
            self.initial_replace
        ))
    }

    pub fn assert_replaced_contains(&self, worker_id: &str) -> Result<()> {
        if self.replaced.iter().any(|id| id == worker_id) {
            return Ok(());
        }
        Err(anyhow!(
            "attempt {}: expected replaced to contain {worker_id}, got {:?}",
            self.attempt_id,
            self.replaced
        ))
    }
}

fn assert_sorted_ids_eq(
    attempt_id: u64,
    label: &str,
    actual: &[String],
    expected: &[&str],
) -> Result<()> {
    let mut actual_sorted = actual.to_vec();
    actual_sorted.sort();
    let mut expected_sorted: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
    expected_sorted.sort();
    if actual_sorted == expected_sorted {
        return Ok(());
    }
    Err(anyhow!(
        "attempt {attempt_id}: {label} mismatch: expected {expected_sorted:?}, got {actual_sorted:?}"
    ))
}

fn attempt_entry(
    by_attempt: &mut BTreeMap<u64, RecoveryAttemptReport>,
    attempt_id: u64,
) -> &mut RecoveryAttemptReport {
    by_attempt.entry(attempt_id).or_insert_with(|| RecoveryAttemptReport {
        attempt_id,
        ..Default::default()
    })
}
