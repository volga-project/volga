//! Kubernetes checkpoint recovery. Require a live cluster; ignored by default.

use anyhow::Result;

use crate::test_utils::checkpoint::{
    assert_checkpoint_multi_restore, assert_checkpoint_restore,
    checkpoint_multi_failure_launch_spec, checkpoint_recovery_launch_spec,
    checkpoint_shared_key_window_launch_spec, kube_checkpoint_spec,
    run_checkpoint_barrier_path, run_checkpoint_mid_flight_kill_after_safe,
    run_checkpoint_mid_flight_kill_no_prior, run_checkpoint_sequential_failures,
    run_checkpoint_worker_kill_recovery, CheckpointWorkload, MULTI_FAILURE_COUNT,
    MULTI_WORKER_PARALLELISM, SINGLE_WORKER_PARALLELISM,
};
use crate::test_utils::harness::{PipelineLaunchSpec, RuntimeEnv, WorkerKillMode};

fn kube_checkpoint_launch(launch: PipelineLaunchSpec) -> PipelineLaunchSpec {
    launch
        .with_kube_worker_health_poll(true)
        .with_state_maintenance(true, 100)
        .with_checkpoint(kube_checkpoint_spec())
}

#[tokio::test]
#[ignore]
async fn test_kube_checkpoint_barrier_path_before_complete() -> Result<()> {
    let checkpoint_id = run_checkpoint_barrier_path(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::PassThrough),
        ),
    )
    .await?;
    assert_eq!(checkpoint_id, 1);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_kube_single_worker_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::PassThrough),
        ),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::PassThrough,
    )
    .await?;
    assert_checkpoint_restore(&report, 1, 1)
}

#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM, CheckpointWorkload::PassThrough),
        ),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::PassThrough,
    )
    .await?;
    assert_checkpoint_restore(&report, 1, 2)
}

#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_sequential_checkpoint_failures_restore() -> Result<()> {
    let report = run_checkpoint_sequential_failures(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(checkpoint_multi_failure_launch_spec()),
        WorkerKillMode::Abrupt,
        MULTI_FAILURE_COUNT,
        CheckpointWorkload::PassThrough,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, MULTI_FAILURE_COUNT, 2)
}

#[tokio::test]
#[ignore]
async fn test_kube_mid_flight_checkpoint_kill_restores_none() -> Result<()> {
    run_checkpoint_mid_flight_kill_no_prior(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::PassThrough),
        ),
        WorkerKillMode::Abrupt,
    )
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_kube_mid_flight_checkpoint_kill_after_safe_restores_prior() -> Result<()> {
    run_checkpoint_mid_flight_kill_after_safe(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::PassThrough),
        ),
        WorkerKillMode::Abrupt,
    )
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_kube_single_worker_window_checkpoint_restore() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::Window),
        ),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::Window,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, 1, 1)
}

#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_window_checkpoint_restore() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(
            checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM, CheckpointWorkload::Window),
        ),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::Window,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, 1, 2)
}

#[tokio::test]
#[ignore]
async fn test_kube_single_worker_window_shared_key_checkpoint_restore() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(checkpoint_shared_key_window_launch_spec(
            SINGLE_WORKER_PARALLELISM,
        )),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::WindowSharedKeys,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, 1, 1)
}

#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_window_shared_key_checkpoint_restore() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        kube_checkpoint_launch(checkpoint_shared_key_window_launch_spec(
            MULTI_WORKER_PARALLELISM,
        )),
        WorkerKillMode::Abrupt,
        CheckpointWorkload::WindowSharedKeys,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, 1, 2)
}
