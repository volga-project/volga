//! Kubernetes checkpoint recovery. Require a live cluster; ignored by default.

use anyhow::Result;

use crate::runtime::tests::checkpoint_tests::{
    assert_checkpoint_restore, checkpoint_recovery_launch_spec, run_checkpoint_barrier_path,
    run_checkpoint_worker_kill_recovery,
};
use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};

#[tokio::test]
#[ignore]
async fn test_kube_checkpoint_barrier_path_before_complete() -> Result<()> {
    let checkpoint_id = run_checkpoint_barrier_path(
        RuntimeEnv::Kube,
        checkpoint_recovery_launch_spec().with_kube_worker_health_poll(true),
    )
    .await?;
    assert_eq!(checkpoint_id, 1);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_kube_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Kube,
        checkpoint_recovery_launch_spec().with_kube_worker_health_poll(true),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_checkpoint_restore(&report, 1)
}
