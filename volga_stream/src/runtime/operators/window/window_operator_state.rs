use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use arrow::array::RecordBatch;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::operators::window::aggregates::merge_accumulator_state;
use crate::runtime::operators::window::batch_index::BatchIndex;
use crate::runtime::operators::window::window_operator::WindowConfig;
use crate::runtime::operators::window::{AggregatorType, TileConfig, Tiles, WindowAggregator, create_window_aggregator};
use crate::runtime::state::OperatorState;
use crate::storage::batch_store::{BatchId, BatchStore, Timestamp};

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

/// Result of inserting a batch - contains normal and late batch IDs
#[derive(Debug, Clone)]
pub struct InsertBatchResult {
    pub normal_batch_ids: Vec<BatchId>,
    pub late_batch_ids: Vec<BatchId>,
}

pub struct WindowsStateGuard {
    _arc: Arc<RwLock<WindowsState>>,
    _guard: tokio::sync::OwnedRwLockReadGuard<WindowsState>,
}

impl WindowsStateGuard {
    pub fn value(&self) -> &WindowsState {
        &*self._guard
    }
}

#[derive(Debug)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
    pub accumulator_state: Option<AccumulatorState>,
    pub end_timestamp: Timestamp,
}

#[derive(Debug)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub batch_index: BatchIndex,
}

#[derive(Debug)]
pub struct WindowOperatorState {
    window_states: DashMap<Key, Arc<RwLock<WindowsState>>>,

    batch_store: Arc<BatchStore>,
}

impl WindowOperatorState {
    pub fn new(batch_store: Arc<BatchStore>) -> Self {
        Self {
            window_states: DashMap::new(),
            batch_store,
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

    pub async fn insert_batch(&self, key: &Key, window_configs: &BTreeMap<WindowId, WindowConfig>, tiling_configs: &Vec<Option<TileConfig>>, ts_column_index: usize, lateness: Option<i64>, batch: RecordBatch) -> InsertBatchResult {
        let window_ids: Vec<_> = window_configs.keys().cloned().collect();
        let window_exprs: Vec<_> = window_configs.values().map(|window| window.window_expr.clone()).collect();
        
        // Get or create windows_state
        let arc_rwlock = if let Some(entry) = self.window_states.get(key) {
            entry.value().clone()
        } else {
            let windows_state = create_empty_windows_state(&window_ids, tiling_configs, &window_exprs);
            let arc_rwlock = Arc::new(RwLock::new(windows_state));
            self.window_states.insert(key.clone(), arc_rwlock.clone());
            arc_rwlock
        };
        
        // Acquire write lock for mutable access
        let mut windows_state = arc_rwlock.write().await;

        let max_timestamp_seen = windows_state.batch_index.max_timestamp_seen();

        // Drop events that are too late based on lateness configuration
        let record_batch = if let Some(lateness_ms) = lateness {
            if max_timestamp_seen > i64::MIN {
                drop_too_late_entries(&batch, ts_column_index, lateness_ms, max_timestamp_seen)
            } else {
                batch.clone()
            }
        } else {
            batch.clone()
        };

        if record_batch.num_rows() == 0 {
            return InsertBatchResult {
                normal_batch_ids: vec![],
                late_batch_ids: vec![],
            };
        } 
        
        // Calculate pre-aggregated tiles if needed
        for (_, window_state) in windows_state.window_states.iter_mut() {
            if let Some(ref mut tiles) = window_state.tiles {
                tiles.add_batch(&record_batch, ts_column_index);
            }
        }

        // Append records to storage (time-partitioned batches)
        let batches = self.batch_store.append_records(record_batch.clone(), key, ts_column_index).await;
        
        // Insert into batch index and track which batches are late
        let mut normal_batch_ids = Vec::new();
        let mut late_batch_ids = Vec::new();
        
        for (batch_id, batch_data) in &batches {
            let (min_ts, max_ts) = get_batch_timestamp_range(&batch_data, ts_column_index);
            let row_count = batch_data.num_rows();
            
            let insert_result = windows_state.batch_index.insert_batch(*batch_id, min_ts, max_ts, row_count);
            
            match insert_result {
                crate::runtime::operators::window::batch_index::InsertResult::Normal => {
                    normal_batch_ids.push(*batch_id);
        }
                crate::runtime::operators::window::batch_index::InsertResult::LateArrival => {
                    late_batch_ids.push(*batch_id);
                }
            }
    }

        // Update accumulators for late arrivals that fall within current window
        if !late_batch_ids.is_empty() {
            Self::update_accumulators_for_late_batches(
                window_configs, 
                &mut *windows_state, 
                &batches, 
                &late_batch_ids, 
                ts_column_index
            );
        }

        InsertBatchResult {
            normal_batch_ids,
            late_batch_ids,
        }
    }

    /// Update accumulators for late batches that fall within current window bounds
    fn update_accumulators_for_late_batches(
        window_configs: &BTreeMap<WindowId, WindowConfig>,
        windows_state: &mut WindowsState,
        batches: &[(BatchId, RecordBatch)],
        late_batch_ids: &[BatchId],
        ts_column_index: usize,
    ) {
        use arrow::compute::{and, filter_record_batch};
        use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
        use arrow::array::{TimestampMillisecondArray, Scalar};
        use crate::runtime::operators::window::batch_index::get_window_length_ms;
        
        // Create a map for quick lookup
        let batch_map: HashMap<BatchId, &RecordBatch> = batches.iter()
            .map(|(id, batch)| (*id, batch))
            .collect();
        
        for (window_id, window_config) in window_configs {
            let aggregator_type = window_config.aggregator_type;
            if aggregator_type != AggregatorType::RetractableAccumulator {
                continue;
            }
            
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            
            let accumulator_state = match window_state.accumulator_state.as_ref() {
                Some(state) => state,
                None => continue,
            };
            
            let window_frame = window_config.window_expr.get_window_frame();
            let window_end = window_state.end_timestamp;
            let window_start = window_end - get_window_length_ms(window_frame);

            // Process each late batch
            for late_batch_id in late_batch_ids {
                let record_batch = match batch_map.get(late_batch_id) {
                    Some(batch) => *batch,
                    None => continue,
                };
            
            let ts_column = record_batch.column(ts_column_index);
            
                let start_array = TimestampMillisecondArray::from_value(window_start, 1);
                let end_array = TimestampMillisecondArray::from_value(window_end, 1);
            let start_scalar = Scalar::new(&start_array);
            let end_scalar = Scalar::new(&end_array);
            
            let ge_start = gt_eq(ts_column, &start_scalar)
                .expect("Should be able to compare with start timestamp");
            let le_end = lt_eq(ts_column, &end_scalar)
                .expect("Should be able to compare with end timestamp");
            
            let within_window = and(&ge_start, &le_end)
                .expect("Should be able to combine boolean arrays");
            
            if within_window.true_count() == 0 {
                continue;
            }
            
            let filtered_batch = filter_record_batch(record_batch, &within_window)
                .expect("Should be able to filter record batch");

            let mut accumulator = match create_window_aggregator(&window_config.window_expr) {
                WindowAggregator::Accumulator(accumulator) => accumulator,
                WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
            };

            merge_accumulator_state(accumulator.as_mut(), accumulator_state);

            let update_args = window_config.window_expr.evaluate_args(&filtered_batch)
                .expect("Should be able to evaluate window args");

            accumulator.update_batch(&update_args)
                .expect("Should be able to update accumulator");

            let window_state = windows_state.window_states.get_mut(window_id).expect("Window state should exist");
            window_state.accumulator_state = Some(accumulator.state().expect("Should be able to get accumulator state"));
            }
        }
    }

    pub async fn prune(&self, key: &Key, lateness: i64, window_configs: &BTreeMap<WindowId, WindowConfig>) {
        use crate::runtime::operators::window::batch_index::get_window_length_ms;
        
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        let mut min_cutoff_timestamp = i64::MAX;

        // For each window state, calculate its specific cutoff and prune tiles
        let window_ids: Vec<WindowId> = windows_state.window_states.keys().cloned().collect();
        for window_id in window_ids {
            let window_config = window_configs.get(&window_id).expect("Window config should exist");
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(&window_id).expect("Window state should exist");
            
            // Calculate cutoff: end_timestamp - window_length - lateness
            let window_length = get_window_length_ms(window_frame);
            let window_cutoff = window_state.end_timestamp - window_length - lateness;

            min_cutoff_timestamp = min_cutoff_timestamp.min(window_cutoff);
            
            if window_cutoff > 0 {
                let window_state = windows_state.window_states.get_mut(&window_id).expect("Window state should exist");
                if let Some(ref mut tiles) = window_state.tiles {
                    tiles.prune(window_cutoff);
                }
            }
        }

        // Use minimal cutoff to prune batch_index and storage
        if min_cutoff_timestamp != i64::MAX && min_cutoff_timestamp > 0 {
            let pruned_batch_ids = windows_state.batch_index.prune(min_cutoff_timestamp);

            if !pruned_batch_ids.is_empty() {
                self.batch_store.remove_batches(&pruned_batch_ids, key).await;
            }
        }
    }

    pub async fn update_window_positions_and_accumulators(
        &self,
        key: &Key,
        new_end_timestamps: &HashMap<WindowId, Timestamp>,
        accumulator_states: &HashMap<WindowId, Option<AccumulatorState>>,
    ) {
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        
        for (window_id, new_end_ts) in new_end_timestamps {
            let window_state = windows_state.window_states.get_mut(window_id).expect("Window state should exist");
            window_state.end_timestamp = *new_end_ts;
            
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
        let windows_state_ref = self.window_states.get(partition_key).unwrap();
        let arc_rwlock = windows_state_ref.value();
        let windows_state = arc_rwlock.read().await;
        
        // Check batch_index buckets are correctly pruned
        let remaining_bucket_timestamps: Vec<_> = windows_state.batch_index.bucket_timestamps();
        
        assert!(remaining_bucket_timestamps.iter().all(|&ts| ts >= expected_min_timestamp),
                "All bucket timestamps should be >= {}ms, found: {:?}", expected_min_timestamp, remaining_bucket_timestamps);
    
        // Check window state and tiles
        for (_, window_state) in windows_state.window_states.iter() {
            if let Some(ref tiles) = window_state.tiles {
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

pub fn create_empty_windows_state(window_ids: &[WindowId], tiling_configs: &[Option<TileConfig>], window_exprs: &[Arc<dyn WindowExpr>]) -> WindowsState {
    WindowsState {
        window_states: window_ids.iter().map(|&window_id| {
            (window_id, WindowState {
                tiles: tiling_configs.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone(), window_exprs[window_id].clone()))),
                accumulator_state: None,
                end_timestamp: i64::MIN,
            })
        }).collect(),
        batch_index: BatchIndex::new(),
    }
}

/// Get min and max timestamps from a batch
fn get_batch_timestamp_range(batch: &RecordBatch, ts_column_index: usize) -> (Timestamp, Timestamp) {
    use arrow::array::TimestampMillisecondArray;
    use arrow::compute::kernels::aggregate::{min, max};
    
    let ts_column = batch.column(ts_column_index);
    let ts_array = ts_column.as_any().downcast_ref::<TimestampMillisecondArray>()
        .expect("Timestamp column should be TimestampMillisecondArray");
    
    let min_ts = min(ts_array).expect("Should have min timestamp");
    let max_ts = max(ts_array).expect("Should have max timestamp");
    
    (min_ts, max_ts)
}

/// Drop entries that are too late based on lateness configuration
pub fn drop_too_late_entries(record_batch: &RecordBatch, ts_column_index: usize, lateness_ms: i64, max_timestamp_seen: Timestamp) -> RecordBatch {
    use arrow::compute::filter_record_batch;
    use arrow::compute::kernels::cmp::gt_eq;
    use arrow::array::{TimestampMillisecondArray, Scalar};
    
    let ts_column = record_batch.column(ts_column_index);
    
    // Calculate cutoff timestamp: max_timestamp_seen - lateness_ms
    let cutoff_timestamp = max_timestamp_seen - lateness_ms;
    
    let cutoff_array = TimestampMillisecondArray::from_value(cutoff_timestamp, 1);
    let cutoff_scalar = Scalar::new(&cutoff_array);
    
    let keep_mask = gt_eq(ts_column, &cutoff_scalar)
        .expect("Should be able to compare with cutoff timestamp");
    
    if keep_mask.true_count() == record_batch.num_rows() {
        record_batch.clone()
    } else {
        filter_record_batch(record_batch, &keep_mask)
            .expect("Should be able to filter record batch")
    }
}