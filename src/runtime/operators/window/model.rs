use std::collections::BTreeMap;

use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::common::Key;
use crate::runtime::utils;

pub type Timestamp = i64;
pub type WindowId = usize;
pub type AccumulatorState = Vec<ScalarValue>;
pub type TileMap = BTreeMap<(TimeGranularity, Timestamp), WindowTiles>;

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

/// Logical namespace shared by WO and WRO.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateNamespace {
    pub bytes: Vec<u8>,
}

impl StateNamespace {
    pub fn new(s: impl AsRef<[u8]>) -> Self {
        Self {
            bytes: s.as_ref().to_vec(),
        }
    }
}

/// Collision-safe logical identity. Backends choose their own physical keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub namespace: Vec<u8>,
    pub business_key: Vec<u8>,
}

impl PartitionKey {
    pub fn new(namespace: &StateNamespace, key: &Key) -> Self {
        Self {
            namespace: namespace.bytes.clone(),
            business_key: key.to_bytes(),
        }
    }
}

/// State published by the WO for one partition.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeyState {
    /// Greatest ingested cursor.
    pub max_seen: Option<Cursor>,
    /// Frontier reflected by WO accumulators and emitted output.
    pub processed_pos: Option<Cursor>,
    #[serde_as(as = "BTreeMap<_, Vec<utils::ScalarValueAsBytes>>")]
    pub accumulators: BTreeMap<WindowId, AccumulatorState>,
    /// Cold-start lower bound; not changed by retention.
    #[serde(default)]
    pub first_ingested: Option<Cursor>,
    /// Per-key sequence allocator, independent of cursor ordering.
    #[serde(default)]
    pub next_seq: u64,
    /// Data before this cursor is outside the supported retention horizon.
    #[serde(default)]
    pub retention_floor: Option<Cursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimeGranularity {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Months(u32),
}

impl TimeGranularity {
    pub fn to_millis(&self) -> i64 {
        match self {
            TimeGranularity::Seconds(s) => *s as i64 * 1000,
            TimeGranularity::Minutes(m) => *m as i64 * 60 * 1000,
            TimeGranularity::Hours(h) => *h as i64 * 60 * 60 * 1000,
            TimeGranularity::Days(d) => *d as i64 * 24 * 60 * 60 * 1000,
            TimeGranularity::Months(m) => *m as i64 * 30 * 24 * 60 * 60 * 1000,
        }
    }

    pub fn is_multiple_of(&self, other: &TimeGranularity) -> bool {
        let self_millis = self.to_millis();
        let other_millis = other.to_millis();
        self_millis > other_millis && self_millis % other_millis == 0
    }

    pub fn start(&self, timestamp: Timestamp) -> Timestamp {
        let duration_millis = self.to_millis();
        (timestamp / duration_millis) * duration_millis
    }

    pub fn next_start(&self, timestamp: Timestamp) -> Timestamp {
        self.start(timestamp) + self.to_millis()
    }

    pub fn prev_start(&self, timestamp: Timestamp) -> Timestamp {
        self.start(timestamp) - self.to_millis()
    }
}

/// Raw segment: half-open `[from, to)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRun {
    pub from: Cursor,
    pub to: Cursor,
}

/// Coalesced tile range at one granularity: half-open `[start_ts, end_ts_exclusive)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TileRun {
    pub granularity: TimeGranularity,
    pub start_ts: Timestamp,
    pub end_ts_exclusive: Timestamp,
}

/// Per-window aggregate state in a shared tile.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TileState {
    #[serde_as(as = "Option<Vec<utils::ScalarValueAsBytes>>")]
    pub accumulator_state: Option<AccumulatorState>,
}

/// Stored value for one `(granularity, tile_start)`: all windows' states.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindowTiles {
    pub windows: BTreeMap<WindowId, TileState>,
}

/// One window's projected tile state for evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tile {
    pub tile_start: Timestamp,
    pub tile_end: Timestamp,
    pub granularity: TimeGranularity,
    pub state: TileState,
}

impl Tile {
    pub fn new(tile_start: Timestamp, granularity: TimeGranularity, state: TileState) -> Self {
        Self {
            tile_start,
            tile_end: tile_start + granularity.to_millis(),
            granularity,
            state,
        }
    }
}
