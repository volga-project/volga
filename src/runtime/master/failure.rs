//! Normalized failure signal for the master lifecycle.

#[derive(Clone, Debug)]
pub(super) enum FailureKind {
    WorkerPanic,
    TaskFailure,
    TransportDisconnect,
    HeartbeatUnavailable,
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
