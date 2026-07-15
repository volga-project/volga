//! Shared recovery-test runner and assertions (local + kube).

pub mod kube;
pub mod local;

use anyhow::Result;
use std::time::Duration;

use crate::api::spec::connectors::SourceSpecKind;
use crate::api::TaskWorkerAssignmentStrategyType;
use crate::runtime::master::LifecycleEvent;
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
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

pub async fn run_worker_kill_recovery(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<(String, RecoveryReport)> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let target_worker = cluster
        .worker_ids()
        .into_iter()
        .next()
        .expect("recovery cluster must have a worker");
    let mut cursor = 0;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.register,
        |event| {
            matches!(
                event,
                LifecycleEvent::WorkerRegistered { worker_id, .. } if worker_id == &target_worker
            )
        },
    )
    .await?;
    cluster.start_execution().await?;

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id: 0, .. }),
    )
    .await?;
    cluster
        .worker(&target_worker)
        .unwrap()
        .kill_with(mode)
        .await?;

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.failure,
        |event| matches!(event, LifecycleEvent::WorkerFailure { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.recovery_started,
        |event| matches!(event, LifecycleEvent::RecoveryStarted { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.replacement,
        |event| {
            matches!(
                event,
                LifecycleEvent::ReplacementRequested { worker_ids }
                    if worker_ids.iter().any(|worker_id| worker_id == &target_worker)
            )
        },
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.re_register,
        |event| {
            matches!(
                event,
                LifecycleEvent::WorkerRegistered { worker_id, .. } if worker_id == &target_worker
            )
        },
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt1_running,
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id, .. } if *attempt_id >= 1),
    )
    .await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    cluster.shutdown().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok((target_worker, RecoveryReport::from_events(&events)))
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

pub fn assert_multi_worker_crash(target: &str, report: &RecoveryReport) -> Result<()> {
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
