use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::{TileConfig, Tiles};
use crate::runtime::state::OperatorState;
use crate::storage::batch_store::{BatchId, BatchStore};

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

#[derive(Debug, Clone)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
    pub accumulator_state: Option<AccumulatorState>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[derive(Debug, Clone)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub time_entries: TimeEntries,
}

#[derive(Debug)]
pub struct WindowOperatorState {
    window_states: DashMap<Key, WindowsState>,

    lock_pool: Vec<Arc<RwLock<()>>>,

    batch_store: Arc<BatchStore>,
}

impl WindowOperatorState {
    pub fn new(batch_store: Arc<BatchStore>) -> Self {
        let lock_pool_size = 4096; // pass as param? TODO
        let lock_pool = (0..lock_pool_size)
            .map(|_| Arc::new(RwLock::new(())))
            .collect();
        Self {
            window_states: DashMap::new(),
            lock_pool,
            batch_store,
        }
    }
    
    fn get_key_lock(&self, key: &Key) -> Arc<RwLock<()>> {
        let hash = key.hash();
        let lock_index = (hash as usize) % self.lock_pool.len();
        
        self.lock_pool[lock_index].clone()
    }

    pub async fn insert_windows_state(&self, key: &Key, windows_state: WindowsState) {
        let key_lock = self.get_key_lock(key);
        let _key_guard = key_lock.write().await;
        self.window_states.insert(key.clone(), windows_state);
    }

    pub async fn get_windows_state_clone(&self, key: &Key) -> Option<WindowsState> {
        let key_lock = self.get_key_lock(key);
        let _key_guard = key_lock.read().await;
        self.window_states.get(key).map(|ref_entry| ref_entry.value().clone())
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

    pub fn get_batch_store(&self) -> &Arc<BatchStore> {
        &self.batch_store
    }
}

impl OperatorState for WindowOperatorState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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