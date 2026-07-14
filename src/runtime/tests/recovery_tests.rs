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

// tests pass individually but can fail when running the full suite (likely cross-test interference).

#[tokio::test]
async fn test_local_single_worker_panic_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        single_worker_recovery_launch_spec(),
        WorkerKillMode::Panic,
    )
    .await?;
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(&target, "WorkerPanic")?;
    attempt0.assert_initial_replace_contains(&target)?;
    attempt0.assert_replaced_contains(&target)?;
    assert!(
        report.attempt(1)?.events.iter().any(|e| e.starts_with("running ")),
        "attempt 1 should reach running"
    );
    Ok(())
}

#[tokio::test]
async fn test_local_single_worker_silent_fail_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        single_worker_recovery_launch_spec(),
        WorkerKillMode::Abrupt,
    )
    .await?;
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(&target, "HeartbeatUnavailable")?;
    attempt0.assert_initial_replace_contains(&target)?;
    attempt0.assert_replaced_contains(&target)?;
    assert!(
        report.attempt(1)?.events.iter().any(|e| e.starts_with("running ")),
        "attempt 1 should reach running"
    );
    Ok(())
}

#[tokio::test]
async fn test_local_multi_worker_crash_recovers() -> Result<()> {
    let (target, report) =
        run_worker_kill_recovery(multi_worker_recovery_launch_spec(), WorkerKillMode::Abrupt)
            .await?;
    report.print();
    report.assert_attempt_count(2)?;
    let attempt0 = report.attempt(0)?;
    attempt0.assert_has_failure(&target, "HeartbeatUnavailable")?;
    attempt0.assert_failure_kind_distinct_workers("TransportDisconnect", 2)?;
    for peer in attempt0.workers.iter().filter(|id| *id != &target) {
        attempt0.assert_has_failure(peer, "TransportDisconnect")?;
    }
    let peers: Vec<&str> = attempt0
        .workers
        .iter()
        .filter(|id| *id != &target)
        .map(String::as_str)
        .collect();
    attempt0.assert_initial_replace_eq(&[&target])?;
    attempt0.assert_replaced_eq(&[&target])?;
    attempt0.assert_reused_eq(&peers)?;
    assert!(
        report.attempt(1)?.events.iter().any(|e| e.starts_with("running ")),
        "attempt 1 should reach running"
    );
    Ok(())
}

async fn run_worker_kill_recovery(
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<(String, RecoveryReport)> {
    let cluster = TestCluster::launch(RuntimeEnv::Local, launch).await?;
    let target_worker = cluster
        .worker_ids()
        .into_iter()
        .next()
        .expect("recovery cluster must have a worker");
    let mut cursor = 0;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(2),
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
        Duration::from_secs(10),
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
        Duration::from_secs(20),
        |event| matches!(event, LifecycleEvent::WorkerFailure { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(10),
        |event| matches!(event, LifecycleEvent::RecoveryStarted { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(20),
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
        Duration::from_secs(10),
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
        Duration::from_secs(20),
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id, .. } if *attempt_id >= 1),
    )
    .await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    cluster.shutdown().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok((target_worker, RecoveryReport::from_events(&events)))
}

fn single_worker_recovery_launch_spec() -> PipelineLaunchSpec {
    let mut launch = smoke_launch_spec(TaskWorkerAssignmentStrategyType::SingleWorker, 2, 1);
    apply_streaming_recovery_source(&mut launch);
    launch
}

fn multi_worker_recovery_launch_spec() -> PipelineLaunchSpec {
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
