use anyhow::Result;

use crate::runtime::tests::checkpoint_tests::{
    assert_checkpoint_multi_restore, assert_checkpoint_restore,
    checkpoint_multi_failure_launch_spec, checkpoint_recovery_launch_spec, MULTI_FAILURE_COUNT,
    MULTI_WORKER_PARALLELISM, SINGLE_WORKER_PARALLELISM, run_checkpoint_barrier_path,
    run_checkpoint_mid_flight_kill_after_safe, run_checkpoint_mid_flight_kill_no_prior,
    run_checkpoint_sequential_failures, run_checkpoint_worker_kill_recovery,
};
use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};

#[tokio::test]
async fn test_local_checkpoint_barrier_path_before_complete() -> Result<()> {
    let checkpoint_id = run_checkpoint_barrier_path(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM),
    )
    .await?;
    assert_eq!(checkpoint_id, 1);
    Ok(())
}

#[tokio::test]
async fn test_local_single_worker_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM),
        WorkerKillMode::Abrupt,
    )
    .await?;
    // Checkpoint ids are monotonic from the coordinator (first complete is 1).
    assert_checkpoint_restore(&report, 1, 1)
}

#[tokio::test]
async fn test_local_multi_worker_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_checkpoint_restore(&report, 1, 2)
}

#[tokio::test]
async fn test_local_multi_worker_sequential_checkpoint_failures_restore() -> Result<()> {
    let report = run_checkpoint_sequential_failures(
        RuntimeEnv::Local,
        checkpoint_multi_failure_launch_spec(),
        WorkerKillMode::Abrupt,
        MULTI_FAILURE_COUNT,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, MULTI_FAILURE_COUNT, 2)
}

#[tokio::test]
async fn test_local_mid_flight_checkpoint_kill_restores_none() -> Result<()> {
    run_checkpoint_mid_flight_kill_no_prior(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM),
        WorkerKillMode::Abrupt,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn test_local_mid_flight_checkpoint_kill_after_safe_restores_prior() -> Result<()> {
    run_checkpoint_mid_flight_kill_after_safe(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM),
        WorkerKillMode::Abrupt,
    )
    .await?;
    Ok(())
}
