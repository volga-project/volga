use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkAssignConfig {
    pub out_of_orderness_ms: u64,
    pub time_hint: TimeHint,
    pub idle_timeout_ms: Option<u64>,
    /// Processing-time coalesce. `Duration::ZERO` emits on every advance (tests only).
    pub emit_interval: Duration,
}

impl WatermarkAssignConfig {
    pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 1_000;
    pub const DEFAULT_STREAMING_EMIT_INTERVAL: Duration = Duration::from_millis(200);
    pub const DEFAULT_REQUEST_EMIT_INTERVAL: Duration = Duration::from_millis(20);

    pub fn new(out_of_orderness_ms: u64, time_hint: TimeHint) -> Self {
        Self {
            out_of_orderness_ms,
            time_hint,
            idle_timeout_ms: Some(Self::DEFAULT_IDLE_TIMEOUT_MS),
            // Tests / direct construction: emit on every advance. Planner fills
            // streaming/request defaults; do not treat zero as a user-facing spec value.
            emit_interval: Duration::ZERO,
        }
    }

    pub fn with_emit_interval(mut self, emit_interval: Duration) -> Self {
        self.emit_interval = emit_interval;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeHint {
    ColumnName { name: String },
}
