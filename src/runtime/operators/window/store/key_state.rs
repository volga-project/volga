use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};
use crate::runtime::utils;

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
