#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkAssignConfig {
    pub out_of_orderness_ms: u64,
    pub time_hint: Option<TimeHint>,
}

impl WatermarkAssignConfig {
    pub fn new(out_of_orderness_ms: u64, time_hint: Option<TimeHint>) -> Self {
        Self {
            out_of_orderness_ms,
            time_hint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeHint {
    WindowOrderByColumn,
    ColumnName { name: String },
    // TODO: auto-resolve for non-window operators (timestamp-like column heuristics).
}

