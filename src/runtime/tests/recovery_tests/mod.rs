//! Shared recovery-test runner and assertions (local + kube).

pub mod kube;
pub mod local;

use anyhow::Result;
use std::collections::HashSet;
use std::time::Duration;

use crate::api::spec::connectors::SourceSpecKind;
use crate::api::TaskWorkerAssignmentStrategyType;
use crate::runtime::master::LifecycleEvent;
use crate::runtime::tests::cluster_harness::{
    MasterHandle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::runtime::tests::launch_specs::{
    default_pipelined_smoke_launch_spec, smoke_launch_spec,
};

#[derive(Clone, Copy)]
pub struct RecoveryTimeouts {
    pub register: Duration,
    pub attempt_running: Duration,
    pub failure: Duration,
    pub recovery_started: Duration,
    pub replacement: Duration,
    pub re_register: Duration,
    pub attempt1_running: Duration,
}

impl RecoveryTimeouts {
    pub fn for_env(env: RuntimeEnv) -> Self {
        match env {
            RuntimeEnv::Local => Self {
                register: Duration::from_secs(2),
                attempt_running: Duration::from_secs(10),
                failure: Duration::from_secs(20),
                recovery_started: Duration::from_secs(10),
                replacement: Duration::from_secs(20),
                re_register: Duration::from_secs(10),
                attempt1_running: Duration::from_secs(20),
            },
            RuntimeEnv::Kube | RuntimeEnv::Docker => Self {
                register: Duration::from_secs(60),
                attempt_running: Duration::from_secs(60),
                failure: Duration::from_secs(60),
                recovery_started: Duration::from_secs(60),
                replacement: Duration::from_secs(120),
                re_register: Duration::from_secs(120),
                attempt1_running: Duration::from_secs(120),
            },
        }
    }
}

/// Kill the first worker in a cluster and wait for recovery through attempt ≥ 1.
pub async fn run_worker_kill_recovery(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<(String, RecoveryReport)> {
    let (targets, report) = run_workers_kill_recovery(env, launch, mode, 1).await?;
    Ok((
        targets
            .into_iter()
            .next()
            .expect("single-kill recovery must return one target"),
        report,
    ))
}

/// Kill the first `kill_count` workers (by cluster worker-id order) and wait for recovery.
pub async fn run_workers_kill_recovery(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
    kill_count: usize,
) -> Result<(Vec<String>, RecoveryReport)> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let worker_ids = cluster.worker_ids();
    assert!(
        kill_count >= 1 && kill_count <= worker_ids.len(),
        "kill_count={kill_count} out of range for {} workers",
        worker_ids.len()
    );
    let targets: Vec<String> = worker_ids.iter().take(kill_count).cloned().collect();
    let target_set: HashSet<String> = targets.iter().cloned().collect();
    let all_workers: HashSet<String> = worker_ids.iter().cloned().collect();
    let mut cursor = 0;

    // Local: starts lifecycle (workers already registered at launch). Kube: no-op.
    cluster.start_execution().await?;

    // Require every worker registered AND attempt 0 running, in one pass.
    // A separate "wait for all registers" that drains each since() batch would
    // advance the cursor past AttemptRunning on kube (full journal in one poll).
    if let Err(error) = wait_until_attempt0_ready(
        &cluster.master(),
        &mut cursor,
        timeouts.register + timeouts.attempt_running,
        &all_workers,
        &target_set,
    )
    .await
    {
        dump_recovery_timeline(&cluster.master()).await;
        let _ = cluster.shutdown().await;
        return Err(error);
    }

    // Kill concurrently so cascade fatals land in the same aggregation window.
    let kills = targets.iter().map(|worker_id| {
        let worker = cluster.worker(worker_id).expect("kill target must exist");
        async move { worker.kill_with(mode).await }
    });
    for result in futures::future::join_all(kills).await {
        result?;
    }

    if let Err(error) = wait_until_targets_recovered(
        &cluster.master(),
        &mut cursor,
        &timeouts,
        &target_set,
    )
    .await
    {
        // Timeouts skip assert_* (which prints the report); dump timeline here.
        dump_recovery_timeline(&cluster.master()).await;
        let _ = cluster.shutdown().await;
        return Err(error);
    }

    let events = cluster.master().lifecycle_events_since(0).await?;
    cluster.shutdown().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok((targets, RecoveryReport::from_events(&events)))
}

async fn dump_recovery_timeline(master: &MasterHandle) {
    match master.lifecycle_events_since(0).await {
        Ok(events) => {
            RecoveryReport::from_events(&events).print();
            // Ensure summary appears before the anyhow error line under --nocapture.
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        Err(err) => eprintln!("[recovery] failed to dump timeline: {err:#}"),
    }
}

/// Wait until every expected worker has `WorkerRegistered` and attempt 0 is running
/// with `kill_targets` present. Tracks both conditions in one cursor scan so a late
/// kube attach (full journal in the first poll) cannot skip `AttemptRunning`.
async fn wait_until_attempt0_ready(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    expected_workers: &HashSet<String>,
    kill_targets: &HashSet<String>,
) -> Result<()> {
    let mut pending_register = expected_workers.clone();
    let mut attempt0_running = false;
    let started = tokio::time::Instant::now();

    while !(pending_register.is_empty() && attempt0_running) {
        for record in master.lifecycle_events_since(*cursor).await? {
            *cursor = record.sequence;
            match &record.event {
                LifecycleEvent::WorkerRegistered { worker_id } => {
                    pending_register.remove(worker_id);
                }
                LifecycleEvent::AttemptRunning {
                    attempt_id: 0,
                    worker_ids,
                } if kill_targets.iter().all(|t| worker_ids.contains(t)) => {
                    attempt0_running = true;
                }
                _ => {}
            }
        }
        if pending_register.is_empty() && attempt0_running {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(anyhow::anyhow!(
                "timed out waiting for attempt 0 ready; still_unregistered={pending_register:?} \
                 attempt0_running={attempt0_running} kill_targets={kill_targets:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

/// Wait until every kill target has been replaced (possibly across multiple recovery
/// cycles / teardown expansion) and a later attempt is running.
async fn wait_until_targets_recovered(
    master: &MasterHandle,
    cursor: &mut u64,
    timeouts: &RecoveryTimeouts,
    targets: &HashSet<String>,
) -> Result<()> {
    let mut replaced: HashSet<String> = HashSet::new();
    let mut saw_attempt_ge_1 = false;
    let started = tokio::time::Instant::now();
    let timeout = timeouts.replacement + timeouts.re_register + timeouts.attempt1_running;

    while !(targets.is_subset(&replaced) && saw_attempt_ge_1) {
        for record in master.lifecycle_events_since(*cursor).await? {
            *cursor = record.sequence;
            match &record.event {
                LifecycleEvent::ReplacementRequested { worker_ids } => {
                    replaced.extend(worker_ids.iter().cloned());
                }
                LifecycleEvent::AttemptRunning { attempt_id, .. } if *attempt_id >= 1 => {
                    saw_attempt_ge_1 = true;
                }
                _ => {}
            }
        }
        if targets.is_subset(&replaced) && saw_attempt_ge_1 {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(anyhow::anyhow!(
                "timed out waiting for kill recovery; replaced={replaced:?} \
                 saw_attempt_ge_1={saw_attempt_ge_1} targets={targets:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

pub fn assert_single_worker_panic(target: &str, report: &RecoveryReport) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(target, "WorkerPanic")?;
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

pub fn assert_single_worker_silent_fail(target: &str, report: &RecoveryReport) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_any_failure(target, &["HeartbeatUnavailable", "StatePollFailure"])?;
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

/// Dead-peer detection via heartbeat reconnect budget (not state-poll race).
pub fn assert_single_worker_heartbeat_unavailable(
    target: &str,
    report: &RecoveryReport,
) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(target, "HeartbeatUnavailable")?;
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

/// Dead-peer detection via `get_worker_state` in the `run()` loop.
pub fn assert_single_worker_state_poll_failure(
    target: &str,
    report: &RecoveryReport,
) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(target, "StatePollFailure")?;
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

/// Assert recovery was triggered by **attempt fencing** after `SameAddrRestart`:
/// the master's cached address becomes reachable again, but the new process is not
/// bound to the running attempt. A plain dead-peer timeout (no fence err msg) must
/// not satisfy this check.
pub fn assert_single_worker_same_addr_restart_fenced(
    target: &str,
    report: &RecoveryReport,
) -> Result<()> {
    use crate::common::failure::{HEARTBEAT_FENCE_ERR_MSG, STATE_POLL_FENCE_ERR_MSGS};

    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    let fenced = attempt0.failures.iter().any(|f| {
        f.worker_id == target
            && match f.kind.as_str() {
                "HeartbeatUnavailable" => f.detail.contains(HEARTBEAT_FENCE_ERR_MSG),
                "StatePollFailure" => STATE_POLL_FENCE_ERR_MSGS
                    .iter()
                    .any(|m| f.detail.contains(m)),
                _ => false,
            }
    });
    if !fenced {
        return Err(anyhow::anyhow!(
            "attempt {}: expected fence rejection for {target} (HB '{}' or StatePollFailure {:?}), got {:?}",
            attempt0.attempt_id,
            HEARTBEAT_FENCE_ERR_MSG,
            STATE_POLL_FENCE_ERR_MSGS,
            attempt0
                .failures
                .iter()
                .map(|f| format!("{}:{} ({})", f.worker_id, f.kind, f.detail))
                .collect::<Vec<_>>()
        ));
    }
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

pub fn assert_single_worker_pod_unhealthy(target: &str, report: &RecoveryReport) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(target, "PodUnhealthy")?;
    attempt0.assert_initial_replace_contains(target)?;
    attempt0.assert_replaced_contains(target)?;
    assert_attempt1_running(report)
}

/// Multi-worker cluster, kill one worker: primary fatal on target, transport cascade on peers,
/// only the killed worker replaced.
pub fn assert_multi_worker_single_kill(target: &str, report: &RecoveryReport) -> Result<()> {
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_any_failure(
        target,
        &["PodUnhealthy", "HeartbeatUnavailable", "StatePollFailure"],
    )?;
    attempt0.assert_failure_kind_distinct_workers("TransportDisconnect", 2)?;
    for peer in attempt0.workers.iter().filter(|id| *id != target) {
        attempt0.assert_has_failure(peer, "TransportDisconnect")?;
    }
    let peers: Vec<&str> = attempt0
        .workers
        .iter()
        .filter(|id| *id != target)
        .map(String::as_str)
        .collect();
    attempt0.assert_initial_replace_eq(&[target])?;
    attempt0.assert_replaced_eq(&[target])?;
    attempt0.assert_reused_eq(&peers)?;
    assert_attempt1_running(report)
}

/// Multi-worker cluster, kill several workers: every target is replaced (possibly across
/// recovery cycles / `reset_worker` teardown expansion). Survivor reuse is soft-checked
/// (warn only) — reset can fail under load and expand replace. Does **not** require a
/// primary fatal (HB/Panic/PodUnhealthy) per killed worker — TD can win the race.
pub fn assert_multi_worker_multi_kill(targets: &[String], report: &RecoveryReport) -> Result<()> {
    report.print();
    let max_attempt = report
        .attempts
        .keys()
        .max()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("expected recovery attempts in report"))?;
    if max_attempt < 1 {
        return Err(anyhow::anyhow!(
            "expected attempt >= 1 after multi-kill recovery, max={max_attempt}"
        ));
    }

    let target_set: HashSet<&str> = targets.iter().map(String::as_str).collect();
    let mut replaced_union: HashSet<&str> = HashSet::new();
    for attempt in report.attempts.values() {
        replaced_union.extend(attempt.replaced.iter().map(String::as_str));
    }
    for target in targets {
        if !replaced_union.contains(target.as_str()) {
            return Err(anyhow::anyhow!(
                "expected {target} to be replaced at least once; replaced_union={replaced_union:?}"
            ));
        }
        // Do not require a primary fatal per killed worker: TD can win the race, and a
        // dead peer may only enter `replaced` via reset_worker teardown expansion.
    }

    let workers = &report.attempt(0)?.workers;
    let survivors: Vec<&str> = workers
        .iter()
        .filter(|id| !target_set.contains(id.as_str()))
        .map(String::as_str)
        .collect();
    assert!(
        !survivors.is_empty(),
        "multi-kill assert expects at least one surviving worker"
    );
    // Soft: survivor reuse is desirable but reset can fail under multi-kill load and
    // correctly expand replace. Single-kill multi-worker remains the hard reuse contract.
    for peer in &survivors {
        if replaced_union.contains(peer) {
            eprintln!(
                "[SOFT] survivor {peer} was replaced (expected reuse); \
                 replaced_union={replaced_union:?} survivors={survivors:?}"
            );
        }
    }

    assert!(
        report
            .attempt(max_attempt)?
            .events
            .iter()
            .any(|e| e.starts_with("running ")),
        "attempt {max_attempt} should reach running"
    );
    Ok(())
}

fn assert_attempt1_running(report: &RecoveryReport) -> Result<()> {
    assert!(
        report
            .attempt(1)?
            .events
            .iter()
            .any(|e| e.starts_with("running ")),
        "attempt 1 should reach running"
    );
    Ok(())
}

pub fn single_worker_recovery_launch_spec() -> PipelineLaunchSpec {
    let mut launch = smoke_launch_spec(TaskWorkerAssignmentStrategyType::SingleWorker, 2, 1);
    apply_streaming_recovery_source(&mut launch);
    launch
}

pub fn multi_worker_recovery_launch_spec() -> PipelineLaunchSpec {
    let mut launch = default_pipelined_smoke_launch_spec();
    apply_streaming_recovery_source(&mut launch);
    launch
}

fn apply_streaming_recovery_source(launch: &mut PipelineLaunchSpec) {
    launch.expected_output_rows = 0;
    if let SourceSpecKind::Datagen(datagen) = &mut launch.pipeline.sources[0].source {
        datagen.rate = Some(50.0);
        datagen.limit = None;
    }
}
