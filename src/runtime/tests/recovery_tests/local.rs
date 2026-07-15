use anyhow::Result;

use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};
use crate::runtime::tests::recovery_tests::{
    assert_multi_worker_crash, assert_single_worker_panic,
    assert_single_worker_same_addr_restart_fenced, assert_single_worker_silent_fail,
    multi_worker_recovery_launch_spec, run_worker_kill_recovery, single_worker_recovery_launch_spec,
};

// TODO add a test whete schedule failure leads to recovery and detects replacable workers

#[tokio::test]
async fn test_local_single_worker_panic_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Local,
        single_worker_recovery_launch_spec(),
        WorkerKillMode::Panic,
    )
    .await?;
    assert_single_worker_panic(&target, &report)
}

#[tokio::test]
async fn test_local_single_worker_silent_fail_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Local,
        single_worker_recovery_launch_spec(),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_single_worker_silent_fail(&target, &report)
}

/// After kill, a new process binds the same addr but is not configured for attempt 0
/// (like kube recreating a pod that reuses the IP). Master must fence that peer and
/// recover — not hang treating the unbound worker as healthy.
#[tokio::test]
async fn test_local_single_worker_same_addr_restart_fenced_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Local,
        single_worker_recovery_launch_spec(),
        WorkerKillMode::SameAddrRestart,
    )
    .await?;
    assert_single_worker_same_addr_restart_fenced(&target, &report)
}

#[tokio::test]
async fn test_local_multi_worker_crash_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Local,
        multi_worker_recovery_launch_spec(),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_multi_worker_crash(&target, &report)
}
