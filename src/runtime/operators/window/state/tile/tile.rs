use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};
use crate::runtime::utils;

use super::granularity::{TimeGranularity, Timestamp};

/// Per-window aggregate state in a shared tile KV value.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TileState {
    #[serde_as(as = "Option<Vec<utils::ScalarValueAsBytes>>")]
    pub accumulator_state: Option<AccumulatorState>,
    pub entry_count: u64,
}

/// SortedKV value for one `(granularity, tile_start)`: all windows' states.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindowTiles {
    pub windows: BTreeMap<WindowId, TileState>,
}

/// One window's tile view for evaluate (geometry from KV key + that window's state).
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

/// Project one window's [`Tile`]s from loaded [`WindowTiles`] keyed by `(gran, tile_start)`.
pub fn project_tiles(
    by_key: &BTreeMap<(TimeGranularity, i64), WindowTiles>,
    window_id: WindowId,
) -> Vec<Tile> {
    let mut out = Vec::new();
    for ((gran, tile_start), window_tiles) in by_key {
        if let Some(state) = window_tiles.windows.get(&window_id) {
            out.push(Tile::new(*tile_start, *gran, state.clone()));
        }
    }
    out.sort_by_key(|t| (t.tile_start, t.granularity.to_millis()));
    out
}
