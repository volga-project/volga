#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkAssignConfig {
    pub out_of_orderness_ms: u64,
    pub time_hint: Option<TimeHint>,
    pub idle_timeout_ms: Option<u64>,
}

impl WatermarkAssignConfig {
    pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 1_000;

    pub fn new(out_of_orderness_ms: u64, time_hint: Option<TimeHint>) -> Self {
        Self {
            out_of_orderness_ms,
            time_hint,
            idle_timeout_ms: Some(Self::DEFAULT_IDLE_TIMEOUT_MS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeHint {
    WindowOrderByColumn,
    ColumnName { name: String },
    // TODO: auto-resolve for non-window operators (timestamp-like column heuristics).
}

