//! Kubernetes recovery tests. Require a live cluster + manifests; ignored by default.
//!
//! Poll on/off is selected via pipeline annotation `volga.io/kube-worker-health-poll`
//! (see `PipelineLaunchSpec::with_kube_worker_health_poll`). Panic inject is local-only.
//!
//! Uses kube_test runtime consts (`config/runtime_consts.kube_test.json`, same values as prod) —
//! local_test consts are too short for STS ready/register.
//!
//! Naming: `{env}_{topology}_{kill_scope}_{detection}_recovers`
//! - topology: `single_worker` | `multi_worker`
//! - kill_scope (multi only): `single_kill` | `multi_kill`
//! - detection: `kill_pod_poll` | `kill_pod_heartbeat` (omit when poll-on is implied)
//!
//! After attempt is **running**, kill can surface as:
//! - `PodUnhealthy` — kube Ready poller (poll on)
//! - `HeartbeatUnavailable` — HB reconnect budget (poll off; favored by production consts)
//! - `StatePollFailure` — `get_worker_state` in `run()` (same phase as HB, races with it)

use anyhow::Result;

use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};
use crate::runtime::tests::recovery_tests::{
    assert_multi_worker_multi_kill, assert_multi_worker_single_kill,
    assert_single_worker_heartbeat_unavailable, assert_single_worker_pod_unhealthy,
    multi_worker_recovery_launch_spec, run_worker_kill_recovery, run_workers_kill_recovery,
    single_worker_recovery_launch_spec,
};

#[tokio::test]
#[ignore]
async fn test_kube_single_worker_kill_pod_poll_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Kube,
        single_worker_recovery_launch_spec().with_kube_worker_health_poll(true),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_single_worker_pod_unhealthy(&target, &report)
}

/// Poller off: expect HB dead-peer path (production consts bias HB ahead of state poll).
#[tokio::test]
#[ignore]
async fn test_kube_single_worker_kill_pod_heartbeat_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Kube,
        single_worker_recovery_launch_spec().with_kube_worker_health_poll(false),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_single_worker_heartbeat_unavailable(&target, &report)
}

/// Multi-worker cluster: kill one pod (poll on); peers cascade and are reused.
#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_single_kill_pod_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Kube,
        multi_worker_recovery_launch_spec().with_kube_worker_health_poll(true),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_multi_worker_single_kill(&target, &report)
}

/// Multi-worker cluster: kill two of three pods (poll on); both replaced; survivor reuse soft.
#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_multi_kill_pod_recovers() -> Result<()> {
    let (targets, report) = run_workers_kill_recovery(
        RuntimeEnv::Kube,
        multi_worker_recovery_launch_spec().with_kube_worker_health_poll(true),
        WorkerKillMode::Abrupt,
        2,
    )
    .await?;
    assert_multi_worker_multi_kill(&targets, &report)
}
