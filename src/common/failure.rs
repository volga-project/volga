//! Shared failure signals for master recovery and kube worker-health polling.

use std::collections::{HashMap, HashSet};

/// Present in `HeartbeatUnavailable` detail when HB fencing rejects a reachable peer.
pub const HEARTBEAT_FENCE_ERR_MSG: &str = "worker not bound to attempt";

/// Present in `StatePollFailure` detail when get_worker_state fencing rejects an unbound peer.
pub const STATE_POLL_FENCE_ERR_MSGS: &[&str] = &[
    "not configured for this attempt",
    "stale worker command",
];

#[derive(Clone, Debug)]
pub enum FailureKind {
    WorkerPanic,
    TaskFailure,
    TransportDisconnect,
    HeartbeatUnavailable,
    /// Worker pod missing / not Ready (kube health poll).
    PodUnhealthy,
    StatePollFailure,
}

impl FailureKind {
    pub fn requires_replacement(&self) -> bool {
        matches!(
            self,
            Self::WorkerPanic | Self::HeartbeatUnavailable | Self::PodUnhealthy
        )
    }
}

#[derive(Clone, Debug)]
pub struct FailureEvent {
    pub worker_id: String,
    pub kind: FailureKind,
    pub detail: String,
}

/// Group fatals by worker; replace a worker if any of its events requires replacement.
pub fn workers_to_replace(events: &[FailureEvent]) -> HashSet<String> {
    let mut by_worker: HashMap<&str, bool> = HashMap::new();
    for event in events {
        let replace = by_worker.entry(event.worker_id.as_str()).or_insert(false);
        *replace |= event.kind.requires_replacement();
    }
    by_worker
        .into_iter()
        .filter_map(|(worker_id, replace)| replace.then(|| worker_id.to_string()))
        .collect()
}
