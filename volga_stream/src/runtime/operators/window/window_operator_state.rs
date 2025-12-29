use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::operators::window::window_operator::WindowConfig;
use crate::runtime::operators::window::{BucketIndex, Cursor, SEQ_NO_COLUMN_NAME, TileConfig, Tiles, TimeGranularity};
use crate::runtime::operators::window::index::SortedBucketBatch;
use crate::runtime::state::OperatorState;
use crate::storage::batch_store::{
    BatchStore,
    Timestamp,
};

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

#[derive(Debug)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
    pub accumulator_state: Option<AccumulatorState>,
    pub processed_until: Option<Cursor>,
}

#[derive(Debug)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub bucket_index: BucketIndex,
    pub next_seq_no: u64,
}

#[derive(Debug)]
pub struct WindowOperatorState {
    window_states: DashMap<Key, Arc<RwLock<WindowsState>>>,

    batch_store: Arc<BatchStore>,

    sorted_bucket_cache: DashMap<(u64, Timestamp, u64), Arc<SortedBucketBatch>>,

    bucket_granularity: TimeGranularity,
}

impl WindowOperatorState {
    pub fn new(batch_store: Arc<BatchStore>) -> Self {
        let bucket_granularity = batch_store.bucket_granularity();
        Self {
            window_states: DashMap::new(),
            batch_store,
            sorted_bucket_cache: DashMap::new(),
            bucket_granularity,
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

    pub async fn insert_batch(
        &self,
        key: &Key,
        window_configs: &BTreeMap<WindowId, WindowConfig>,
        tiling_configs: &Vec<Option<TileConfig>>,
        ts_column_index: usize,
        lateness: Option<i64>,
        watermark_ts: Option<Timestamp>,
        batch: RecordBatch,
    ) -> usize {
        let window_ids: Vec<_> = window_configs.keys().cloned().collect();
        let window_exprs: Vec<_> = window_configs.values().map(|window| window.window_expr.clone()).collect();
        
        // Get or create windows_state
        let arc_rwlock = if let Some(entry) = self.window_states.get(key) {
            entry.value().clone()
        } else {
            let windows_state = create_empty_windows_state(&window_ids, tiling_configs, &window_exprs, self.bucket_granularity);
            let arc_rwlock = Arc::new(RwLock::new(windows_state));
            self.window_states.insert(key.clone(), arc_rwlock.clone());
            arc_rwlock
        };
        
        // Acquire write lock for mutable access
        let mut windows_state = arc_rwlock.write().await;

        if batch.num_rows() == 0 {
            return 0;
        }

        // Assign per-row seq_no (tie-breaker for same timestamps).
        let start_seq = windows_state.next_seq_no;
        let record_batch_with_seq = append_seq_no_column(&batch, start_seq);
        windows_state.next_seq_no = windows_state
            .next_seq_no
            .saturating_add(record_batch_with_seq.num_rows() as u64);

        // Drop rows according to watermark-based policy:
        // - If watermark is known, drop anything older than (watermark - lateness).
        // - Otherwise: keep all rows (can't classify lateness yet).
        let (record_batch, dropped_rows) = if let (Some(lateness_ms), Some(wm)) = (lateness, watermark_ts) {
            drop_too_late_entries(&record_batch_with_seq, ts_column_index, lateness_ms, wm)
        } else {
            (record_batch_with_seq.clone(), 0)
        };

        if record_batch.num_rows() == 0 {
            return dropped_rows;
        }

        // Calculate pre-aggregated tiles if needed
        for (_, window_state) in windows_state.window_states.iter_mut() {
            if let Some(ref mut tiles) = window_state.tiles {
                tiles.add_batch(&record_batch, ts_column_index);
            }
        }

        // Append records to storage (time-partitioned batches)
        let batches = self.batch_store.append_records(record_batch.clone(), key, ts_column_index).await;
        
        // Insert into bucket index
        for (batch_id, batch_data) in &batches {
            let seq_column_index = get_seq_no_column_index(batch_data)
                .expect("Expected __seq_no column to exist in stored batches");
            let (min_pos, max_pos) = get_batch_rowpos_range(batch_data, ts_column_index, seq_column_index);
            let row_count = batch_data.num_rows();
            
            windows_state
                .bucket_index
                .insert_batch(*batch_id, min_pos, max_pos, row_count);
    }

        // TODO(metrics): report dropped_rows and per-key drops.
        dropped_rows
    }

    pub async fn prune(&self, key: &Key, lateness: i64, window_configs: &BTreeMap<WindowId, WindowConfig>) {
        use crate::runtime::operators::window::index::get_window_length_ms;
        
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        let mut min_cutoff_timestamp = i64::MAX;

        // For each window state, calculate its specific cutoff and prune tiles
        let window_ids: Vec<WindowId> = windows_state.window_states.keys().cloned().collect();
        for window_id in window_ids {
            let window_config = window_configs.get(&window_id).expect("Window config should exist");
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(&window_id).expect("Window state should exist");
            
            // Calculate cutoff: processed_until.ts - window_length - lateness
            let window_length = get_window_length_ms(window_frame);
            let window_end_ts = window_state.processed_until.map(|p| p.ts).unwrap_or(i64::MIN);
            let window_cutoff = window_end_ts - window_length - lateness;

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
            let pruned_batch_ids = windows_state.bucket_index.prune(min_cutoff_timestamp);

            if !pruned_batch_ids.is_empty() {
                self.batch_store.remove_batches(&pruned_batch_ids, key).await;
            }
        }
    }

    pub async fn update_window_positions_and_accumulators(
        &self,
        key: &Key,
        new_processed_until: &HashMap<WindowId, Cursor>,
        accumulator_states: &HashMap<WindowId, Option<AccumulatorState>>,
    ) {
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        
        for (window_id, new_pos) in new_processed_until {
            let window_state = windows_state.window_states.get_mut(window_id).expect("Window state should exist");
            window_state.processed_until = Some(*new_pos);
            
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
        let remaining_bucket_timestamps: Vec<_> = windows_state.bucket_index.bucket_timestamps();
        
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

    pub fn get_sorted_bucket_cache(&self) -> &DashMap<(u64, Timestamp, u64), Arc<SortedBucketBatch>> {
        &self.sorted_bucket_cache
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

pub fn create_empty_windows_state(window_ids: &[WindowId], tiling_configs: &[Option<TileConfig>], window_exprs: &[Arc<dyn WindowExpr>], bucket_granularity: TimeGranularity) -> WindowsState {
    WindowsState {
        window_states: window_ids.iter().map(|&window_id| {
            (window_id, WindowState {
                tiles: tiling_configs.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone(), window_exprs[window_id].clone()))),
                accumulator_state: None,
                processed_until: None,
            })
        }).collect(),
        bucket_index: BucketIndex::new(bucket_granularity),
        next_seq_no: 0,
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

fn append_seq_no_column(batch: &RecordBatch, start_seq: u64) -> RecordBatch {
    use arrow::array::UInt64Array;

    if batch.schema().field_with_name(SEQ_NO_COLUMN_NAME).is_ok() {
        return batch.clone();
    }

    let n = batch.num_rows() as u64;
    let seq = UInt64Array::from_iter_values(start_seq..(start_seq + n));

    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false));
    let schema = Arc::new(Schema::new(fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(seq));

    RecordBatch::try_new(schema, columns).expect("Should be able to append __seq_no column")
}

fn get_seq_no_column_index(batch: &RecordBatch) -> Option<usize> {
    batch.schema()
        .fields()
        .iter()
        .position(|f| f.name() == SEQ_NO_COLUMN_NAME)
}

fn get_batch_rowpos_range(
    batch: &RecordBatch,
    ts_column_index: usize,
    seq_column_index: usize,
) -> (Cursor, Cursor) {
    use arrow::array::{TimestampMillisecondArray, UInt64Array};

    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("Timestamp column should be TimestampMillisecondArray");
    let seq = batch
        .column(seq_column_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("__seq_no column should be UInt64Array");

    let mut min_pos = Cursor::new(i64::MAX, u64::MAX);
    let mut max_pos = Cursor::new(i64::MIN, 0);
    for i in 0..batch.num_rows() {
        let p = Cursor::new(ts.value(i), seq.value(i));
        if p < min_pos {
            min_pos = p;
        }
        if p > max_pos {
            max_pos = p;
        }
    }
    (min_pos, max_pos)
}

/// Drop entries that are too late based on lateness configuration.
///
/// Drops rows where `ts < (watermark_ts - lateness_ms)`.
pub fn drop_too_late_entries(
    record_batch: &RecordBatch,
    ts_column_index: usize,
    lateness_ms: i64,
    watermark_ts: Timestamp,
) -> (RecordBatch, usize) {
    use arrow::compute::filter_record_batch;
    use arrow::compute::kernels::cmp::gt_eq;
    use arrow::array::{TimestampMillisecondArray, Scalar};
    
    let ts_column = record_batch.column(ts_column_index);
    
    // Calculate cutoff timestamp: watermark_ts - lateness_ms
    let cutoff_timestamp = watermark_ts - lateness_ms;
    
    let cutoff_array = TimestampMillisecondArray::from_value(cutoff_timestamp, 1);
    let cutoff_scalar = Scalar::new(&cutoff_array);
    
    let keep_mask = gt_eq(ts_column, &cutoff_scalar)
        .expect("Should be able to compare with cutoff timestamp");
    
    if keep_mask.true_count() == record_batch.num_rows() {
        return (record_batch.clone(), 0);
    }

    let kept = filter_record_batch(record_batch, &keep_mask)
        .expect("Should be able to filter record batch");
    let dropped = record_batch.num_rows() - kept.num_rows();
    (kept, dropped)
}

// Previously we supported a "monotonic cursor" ingest mode (drop out-of-order rows).
// With watermark-only semantics, we don't need it.