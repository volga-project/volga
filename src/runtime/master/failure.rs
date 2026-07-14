//! Normalized failure signal for the master lifecycle.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub(super) enum FailureKind {
    WorkerPanic,
    TaskFailure,
    TransportDisconnect,
    HeartbeatUnavailable,
    StatePoll,
}

impl FailureKind {
    pub(super) fn requires_replacement(&self) -> bool {
        matches!(self, Self::WorkerPanic | Self::HeartbeatUnavailable)
    }
}

#[derive(Clone, Debug)]
pub(super) struct FailureEvent {
    pub worker_id: String,
    pub kind: FailureKind,
    pub detail: String,
}

/// Group fatals by worker; replace a worker if any of its events requires replacement.
pub(super) fn workers_to_replace(events: &[FailureEvent]) -> HashSet<String> {
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
