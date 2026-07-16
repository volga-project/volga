use anyhow::Result;

use crate::runtime::tests::checkpoint_tests::{
    assert_checkpoint_restore, checkpoint_recovery_launch_spec, run_checkpoint_barrier_path,
    run_checkpoint_worker_kill_recovery,
};
use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};

#[tokio::test]
async fn test_local_checkpoint_barrier_path_before_complete() -> Result<()> {
    let checkpoint_id =
        run_checkpoint_barrier_path(RuntimeEnv::Local, checkpoint_recovery_launch_spec()).await?;
    assert_eq!(checkpoint_id, 1);
    Ok(())
}

#[tokio::test]
async fn test_local_checkpoint_complete_then_worker_kill_restores() -> Result<()> {
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Local,
        checkpoint_recovery_launch_spec(),
        WorkerKillMode::Abrupt,
    )
    .await?;
    // Checkpoint ids are monotonic from the coordinator (first complete is 1).
    assert_checkpoint_restore(&report, 1)
}
