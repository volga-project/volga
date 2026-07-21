use serde::{Deserialize, Serialize};

/// Event-time position with per-key sequence tie-break.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cursor {
    pub ts: i64,
    pub seq_no: u64,
}

impl Cursor {
    pub fn new(ts: i64, seq_no: u64) -> Self {
        Self { ts, seq_no }
    }
}
