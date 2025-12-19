use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use arrow::array::RecordBatch;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;
use serde_with::serde_as;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::operators::window::aggregates::merge_accumulator_state;
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::window_operator::{WindowConfig, drop_too_late_entries};
use crate::runtime::operators::window::{AggregatorType, TileConfig, Tiles, WindowAggregator, create_window_aggregator};
use crate::runtime::state::OperatorState;
use crate::storage::batch_store::{BatchId, BatchStore};
use serde::{Serialize, Deserialize};
use crate::runtime::utils;

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

pub struct WindowsStateGuard {
    _arc: Arc<RwLock<WindowsState>>,
    _guard: tokio::sync::OwnedRwLockReadGuard<WindowsState>,
}

impl WindowsStateGuard {
    pub fn value(&self) -> &WindowsState {
        &*self._guard
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
    #[serde_as(as = "Option<Vec<utils::ScalarValueAsBytes>>")]
    pub accumulator_state: Option<AccumulatorState>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub time_entries: TimeEntries,
}

#[derive(Debug)]
pub struct WindowOperatorState {
    window_states: DashMap<Key, Arc<RwLock<WindowsState>>>,

    batch_store: Arc<BatchStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOperatorStateCheckpoint {
    pub keys: Vec<(Vec<u8>, Vec<u8>)>,
}

impl WindowOperatorState {
    pub fn new(batch_store: Arc<BatchStore>) -> Self {
        Self {
            window_states: DashMap::new(),
            batch_store,
        }
    }

    pub fn to_checkpoint(&self) -> WindowOperatorStateCheckpoint {
        let mut keys = Vec::new();
        for entry in self.window_states.iter() {
            let key = entry.key().clone();
            let arc = entry.value().clone();
            let windows_state = arc.try_read().expect("Failed to read windows state for checkpoint");

            let windows_state_bytes =
                bincode::serialize(&*windows_state).expect("Failed to serialize WindowsState checkpoint");

            keys.push((key.to_bytes(), windows_state_bytes));
        }
        WindowOperatorStateCheckpoint { keys }
    }

    pub fn apply_checkpoint(
        &self,
        checkpoint: WindowOperatorStateCheckpoint,
        window_configs: &BTreeMap<WindowId, WindowConfig>,
        tiling_configs: &Vec<Option<TileConfig>>,
    ) {
        let window_ids: Vec<_> = window_configs.keys().cloned().collect();
        
        for (key_bytes, windows_state_bytes) in checkpoint.keys {
            let key = Key::from_bytes(&key_bytes);
            let windows_state: WindowsState =
                bincode::deserialize(&windows_state_bytes).expect("Failed to deserialize WindowsState checkpoint");

            // We still ensure window_ids exist in config; if config mismatch happens, we prefer to fail fast for now.
            for wid in &window_ids {
                if !windows_state.window_states.contains_key(wid) {
                    panic!("Checkpoint is missing window_id={}", wid);
                }
            }

            let arc = Arc::new(RwLock::new(windows_state));
            self.window_states.insert(key, arc);
        }
    }
    
    pub async fn get_windows_state(&self, key: &Key) -> Option<WindowsStateGuard> {
        let arc_rwlock = self.window_states.get(key)?.value().clone();
        let guard = arc_rwlock.clone().read_owned().await;
        Some(WindowsStateGuard {
            _arc: arc_rwlock,
            _guard: guard,
        })
    }

    pub async fn insert_batch(&self, key: &Key, window_configs: &BTreeMap<WindowId, WindowConfig>, tiling_configs: &Vec<Option<TileConfig>>, ts_column_index: usize, lateness: Option<i64>, batch: RecordBatch) -> Vec<TimeIdx> {
        let window_ids: Vec<_> = window_configs.keys().cloned().collect();
        // let window_exprs: Vec<_> = window_configs.values().map(|window| window.window_expr.clone()).collect();
        
        // Get or create windows_state
        let arc_rwlock = if let Some(entry) = self.window_states.get(key) {
            entry.value().clone()
        } else {
            let windows_state = create_empty_windows_state(&window_ids, tiling_configs);
            let arc_rwlock = Arc::new(RwLock::new(windows_state));
            self.window_states.insert(key.clone(), arc_rwlock.clone());
            arc_rwlock
        };
        
        // Acquire write lock for mutable access
        let mut windows_state = arc_rwlock.write().await;

        // all windows should have the same end
        let last_entry_before_update = Some(windows_state.window_states.iter().next().unwrap().1.end_idx);

        // Drop events that are too late based on lateness configuration, if needed
        let record_batch = if let (Some(lateness_ms), Some(last_entry)) = (lateness, last_entry_before_update.clone()) {
            drop_too_late_entries(&batch, ts_column_index, lateness_ms, last_entry)
        } else {
            batch.clone()
        };

        if record_batch.num_rows() == 0 {
            return Vec::new();
        } 
        
        // calculate pre-aggregated tiles if needed, including not-dropped late entries
        for (window_id, window_state) in windows_state.window_states.iter_mut() {
            if let Some(ref mut tiles) = window_state.tiles {
                let window_expr = &window_configs.get(&window_id).expect("Window config should exist").window_expr;
                tiles.add_batch(&record_batch, window_expr, ts_column_index);
            }
        }

        Self::update_accumulators(window_configs, &mut *windows_state, &record_batch, ts_column_index);

        // append records to storage and get inserted idxs
        let batches = self.batch_store.append_records(record_batch.clone(), key, ts_column_index).await;
        let mut inserted_idxs = Vec::new();
        for (batch_id, batch) in batches {
            inserted_idxs.extend(windows_state.time_entries.insert_batch(batch_id, &batch, ts_column_index));
        }
        inserted_idxs
    }

    // TODO what if RANGE window - we also need retracts.
    // TODO should event outisde of window chnage state?
    
    fn update_accumulators(
        window_configs: &BTreeMap<WindowId, WindowConfig>,
        windows_state: &mut WindowsState,
        record_batch: &RecordBatch,
        ts_column_index: usize,
    ) {
        for (window_id, window_config) in window_configs {
            let aggregator_type = window_config.aggregator_type;
            if aggregator_type != AggregatorType::RetractableAccumulator {
                // Only retractable aggs keep accumulator state
                continue;
            }
            
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            
            // Only update if accumulator state already exists
            let accumulator_state = match window_state.accumulator_state.as_ref() {
                Some(state) => state,
                None => continue,
            };
            
            let window_start = window_state.start_idx;
            let window_end = window_state.end_idx;

            // Filter record_batch to get entries within window using SIMD kernels
            use arrow::compute::{and, filter_record_batch};
            use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
            use arrow::array::{TimestampMillisecondArray, Scalar};
            
            let ts_column = record_batch.column(ts_column_index);
            
            // Create scalar arrays with the window bounds using the same data type as the timestamp column
            let start_array = TimestampMillisecondArray::from_value(window_start.timestamp, 1);
            let end_array = TimestampMillisecondArray::from_value(window_end.timestamp, 1);
            let start_scalar = Scalar::new(&start_array);
            let end_scalar = Scalar::new(&end_array);
            
            // Create boolean masks using SIMD kernels
            let ge_start = gt_eq(ts_column, &start_scalar)
                .expect("Should be able to compare with start timestamp");
            let le_end = lt_eq(ts_column, &end_scalar)
                .expect("Should be able to compare with end timestamp");
            
            // Combine conditions with AND
            let within_window = and(&ge_start, &le_end)
                .expect("Should be able to combine boolean arrays");
            
            // Check if any rows match the filter
            if within_window.true_count() == 0 {
                continue;
            }
            
            // Filter the batch using the boolean mask
            let filtered_batch = filter_record_batch(record_batch, &within_window)
                .expect("Should be able to filter record batch");

            // Create accumulator and restore existing state
            let mut accumulator = match create_window_aggregator(&window_config.window_expr) {
                WindowAggregator::Accumulator(accumulator) => accumulator,
                WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
            };

            merge_accumulator_state(accumulator.as_mut(), accumulator_state);

            // Evaluate window expression arguments and update accumulator
            let update_args = window_config.window_expr.evaluate_args(&filtered_batch)
                .expect("Should be able to evaluate window args");

            accumulator.update_batch(&update_args)
                .expect("Should be able to update accumulator");

            // Save updated accumulator state
            let window_state = windows_state.window_states.get_mut(window_id).expect("Window state should exist");
            window_state.accumulator_state =
                Some(accumulator.state().expect("Should be able to get accumulator state"));
        }
    }

    pub async fn prune(&self, key: &Key, lateness: i64, window_configs: &BTreeMap<WindowId, WindowConfig>) {
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        let mut min_cutoff_timestamp = i64::MAX;

        // For each window state, calculate its specific cutoff and prune tiles
        let window_ids: Vec<WindowId> = windows_state.window_states.keys().cloned().collect();
        for window_id in window_ids {
            let window_config = window_configs.get(&window_id).expect("Window config should exist");
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(&window_id).expect("Window state should exist");
            let window_cutoff = windows_state.time_entries.get_cutoff_timestamp(window_frame, window_state, lateness);

            min_cutoff_timestamp = min_cutoff_timestamp.min(window_cutoff);
            
            if window_cutoff > 0 {
                // Prune tiles for this window with its specific cutoff
                let window_state = windows_state.window_states.get_mut(&window_id).expect("Window state should exist");
                if let Some(ref mut tiles) = window_state.tiles {
                    tiles.prune(window_cutoff);
                }
            }
        }

        // Since data in storage is shared between windows,
        // use minimal cutoff (earliest) timestamp to prune time_index and storage
        if min_cutoff_timestamp != i64::MAX && min_cutoff_timestamp > 0 {
            // Prune time index and get list of pruned batch IDs
            let pruned_batch_ids = windows_state.time_entries.prune(min_cutoff_timestamp);

            // Prune storage - remove batches that are no longer needed
            if !pruned_batch_ids.is_empty() {
                // TODO this should be an async removal and also synced with request operator
                // (if this key is being read by request op, we should delay removal of batches that are still used)
                self.batch_store.remove_batches(&pruned_batch_ids, key).await;
            }
        }
    }

    pub async fn update_window_positions_and_accumulators(
        &self,
        key: &Key,
        new_window_positions: &HashMap<WindowId, (TimeIdx, TimeIdx)>,
        accumulator_states: &HashMap<WindowId, Option<AccumulatorState>>,
    ) {
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        
        for (window_id, _) in new_window_positions {
            let window_state = windows_state.window_states.get_mut(window_id).expect("Window state should exist");
            
            if let Some((new_window_start, new_window_end)) = new_window_positions.get(window_id) {
                window_state.start_idx = *new_window_start;
                window_state.end_idx = *new_window_end;
            }
            
            if let Some(accumulator_state) = accumulator_states.get(window_id) {
                window_state.accumulator_state = accumulator_state.clone();
            }
        }
    }

    pub async fn verify_pruning_for_testing(
        &self, 
        partition_key: &Key,
        expected_min_timestamp: i64
    ) {
        // Check time index entries
        let windows_state_ref = self.window_states.get(partition_key).unwrap();
        let arc_rwlock = windows_state_ref.value();
        let windows_state = arc_rwlock.read().await;
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
                let tile_ranges: Vec<_> = tiles.get_tiles_for_range(0, i64::MAX);
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

pub fn create_empty_windows_state(window_ids: &[WindowId], tiling_configs: &[Option<TileConfig>]) -> WindowsState {
    WindowsState {
        window_states: window_ids.iter().map(|&window_id| {
            (window_id, WindowState {
                tiles: tiling_configs.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone()))),
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