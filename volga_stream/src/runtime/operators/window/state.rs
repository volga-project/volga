use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;

use crate::common::Key;
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::{TileConfig, Tiles};
use crate::storage::storage::BatchId;

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

#[derive(Debug)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
    pub accumulator_state: Option<AccumulatorState>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[derive(Debug)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub time_entries: TimeEntries,
}

#[derive(Debug)]
pub struct State {
    window_states: DashMap<Key, WindowsState>,
}

impl State {
    pub fn new() -> Self {
        Self {
            window_states: DashMap::new(),
        }
    }

    pub async fn insert_windows_state(&self, key: &Key, windows_state: WindowsState) {
        self.window_states.insert(key.clone(), windows_state);
    }

    pub async fn take_windows_state(&self, key: &Key) -> Option<WindowsState> {
        if self.window_states.contains_key(key) {
            return Some(self.window_states.remove(key).unwrap().1)
        }
        None
    }

    pub fn verify_pruning_for_testing(
        &self, 
        partition_key: &Key,
        expected_min_timestamp: i64
    ) {
        // Check time index entries
        let windows_state_ref = self.window_states.get(partition_key).unwrap();
        let windows_state = windows_state_ref.value();
        let remaining_entries: Vec<_> = windows_state.time_entries.entries.iter().map(|entry| entry.timestamp).collect();
        
        assert!(remaining_entries.iter().all(|&ts| ts >= expected_min_timestamp), 
                "All time entries should be >= {}ms, found: {:?}", expected_min_timestamp, remaining_entries);
        
        
        // Check batch_ids mapping is correctly pruned
        let remaining_batch_timestamps: Vec<_> = windows_state.time_entries.batch_ids.iter().map(|entry| *entry.key()).collect();
        
        assert!(remaining_batch_timestamps.iter().all(|&ts| ts >= expected_min_timestamp),
                "All batch timestamps should be >= {}ms, found: {:?}", expected_min_timestamp, remaining_batch_timestamps);
    
        // Check window state and tiles
        for (_, window_state) in windows_state.window_states.iter() {
            if let Some(ref tiles) = window_state.tiles {
                // Assert tile boundaries respect the cutoff timestamp
                let tile_ranges = tiles.get_tiles_for_range(0, i64::MAX);
                for tile in tile_ranges {
                    assert!(tile.tile_end > expected_min_timestamp, 
                            "Tile should end after cutoff {}ms, but tile ends at {}ms", 
                            expected_min_timestamp, tile.tile_end);
                }
            }
        }
    }
}

pub fn create_empty_windows_state(window_ids: &[WindowId], tiling_configs: &[Option<TileConfig>], window_exprs: &[Arc<dyn WindowExpr>]) -> WindowsState {
    WindowsState {
        window_states: window_ids.iter().map(|&window_id| {
            (window_id, WindowState {
                tiles: tiling_configs.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone(), window_exprs[window_id].clone()))),
                accumulator_state: None,
                start_idx: TimeIdx { 
                    batch_id: BatchId::random(),
                    timestamp: 0,
                    pos_idx: 0,
                    row_idx: 0,
                },
                end_idx: TimeIdx { 
                    batch_id: BatchId::random(),
                    timestamp: 0,
                    pos_idx: 0,
                    row_idx: 0,
                },
            })
        }).collect(),
        time_entries: TimeEntries::new(),
    }
}