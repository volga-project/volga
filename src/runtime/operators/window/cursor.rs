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

    pub fn next(self) -> Self {
        if self.seq_no == u64::MAX {
            Self::new(self.ts.saturating_add(1), 0)
        } else {
            Self::new(self.ts, self.seq_no + 1)
        }
    }

    /// Exclusive upper cursor containing every event at `ts`.
    pub fn after_timestamp(ts: i64) -> Self {
        if ts == i64::MAX {
            Self::new(i64::MAX, u64::MAX)
        } else {
            Self::new(ts + 1, 0)
        }
    }
}
