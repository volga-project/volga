//! Kubernetes recovery tests. Require a live cluster + manifests; ignored by default.
//!
//! Poll on/off is selected via pipeline annotation `volga.io/kube-worker-health-poll`
//! (see `PipelineLaunchSpec::with_kube_worker_health_poll`). Panic inject is local-only.
//!
//! Uses production runtime consts (`config/runtime_consts.production.json`) — test consts
//! are too short for kube STS ready/register.
//!
//! After attempt is **running**, kill can surface as:
//! - `PodUnhealthy` — kube Ready poller (poll on)
//! - `HeartbeatUnavailable` — HB reconnect budget (poll off; favored by production consts)
//! - `StatePollFailure` — `get_worker_state` in `run()` (same phase as HB, races with it)

use anyhow::Result;

use crate::runtime::tests::cluster_harness::{RuntimeEnv, WorkerKillMode};
use crate::runtime::tests::recovery_tests::{
    assert_multi_worker_crash, assert_single_worker_heartbeat_unavailable,
    assert_single_worker_pod_unhealthy, multi_worker_recovery_launch_spec,
    run_worker_kill_recovery, single_worker_recovery_launch_spec,
};

#[tokio::test]
#[ignore]
async fn test_kube_single_worker_kill_pod_watcher_recovers() -> Result<()> {
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

#[tokio::test]
#[ignore]
async fn test_kube_multi_worker_kill_pod_recovers() -> Result<()> {
    let (target, report) = run_worker_kill_recovery(
        RuntimeEnv::Kube,
        multi_worker_recovery_launch_spec().with_kube_worker_health_poll(true),
        WorkerKillMode::Abrupt,
    )
    .await?;
    assert_multi_worker_crash(&target, &report)
}
