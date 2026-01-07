use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::runtime::operators::window::window_operator::WindowConfig;
use crate::runtime::operators::window::aggregates::BucketRange;
use crate::runtime::operators::window::{BucketIndex, Cursor, SEQ_NO_COLUMN_NAME, TileConfig, Tiles, TimeGranularity};
use crate::storage::index::bucket_index::BatchRef;
use crate::runtime::state::OperatorState;
use crate::storage::InMemBatchCache;
use crate::runtime::TaskId;
use crate::storage::{BatchPins, StorageStatsSnapshot, WorkerStorageContext};
use crate::storage::batch_store::{
    BatchStore,
    Timestamp,
};
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
    pub processed_until: Option<Cursor>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub bucket_index: BucketIndex,
    pub next_seq_no: u64,
}

#[derive(Debug)]
pub struct WindowOperatorState {
    window_states: DashMap<Key, Arc<RwLock<WindowsState>>>,

    storage: Arc<WorkerStorageContext>,
    task_id: TaskId,
    batch_reclaimer_started: AtomicBool,
    background_tasks_started: AtomicBool,
    batch_reclaimer_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    compaction_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    dump_handle: std::sync::Mutex<Option<JoinHandle<()>>>,

    bucket_granularity: TimeGranularity,

    // -------- stable operator config (owned by state) --------
    ts_column_index: usize,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    window_ids: Vec<WindowId>,
    tiling_configs: Vec<Option<TileConfig>>,
    lateness: Option<i64>,

    // Backpressure / dump policy knobs.
    dump_hot_bucket_count: usize,
    in_mem_low_watermark_per_mille: u32,
    in_mem_dump_parallelism: usize,
}

// Compaction and dumping are intentionally split:
// - compaction (read-triggered) publishes in-memory runs only (no store writes)
// - dumping is the only codepath that writes to `BatchStore` and publishes `BatchRef::Stored`

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOperatorStateCheckpoint {
    pub keys: Vec<(Vec<u8>, Vec<u8>)>,
}

impl WindowOperatorState {
    pub fn new(
        storage: Arc<WorkerStorageContext>,
        task_id: TaskId,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        tiling_configs: Vec<Option<TileConfig>>,
        lateness: Option<i64>,
        dump_hot_bucket_count: usize,
        in_mem_low_watermark_per_mille: u32,
        in_mem_dump_parallelism: usize,
    ) -> Self {
        let bucket_granularity = storage.batch_store.bucket_granularity();
        let window_ids: Vec<WindowId> = window_configs.keys().cloned().collect();
        Self {
            window_states: DashMap::new(),
            storage,
            task_id,
            batch_reclaimer_started: AtomicBool::new(false),
            background_tasks_started: AtomicBool::new(false),
            batch_reclaimer_handle: std::sync::Mutex::new(None),
            compaction_handle: std::sync::Mutex::new(None),
            dump_handle: std::sync::Mutex::new(None),
            bucket_granularity,
            ts_column_index,
            window_configs,
            window_ids,
            tiling_configs,
            lateness,
            dump_hot_bucket_count,
            in_mem_low_watermark_per_mille,
            in_mem_dump_parallelism,
        }
    }

    pub fn storage(&self) -> &Arc<WorkerStorageContext> {
        &self.storage
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    pub fn in_mem_batch_cache(&self) -> &InMemBatchCache {
        &self.storage.in_mem
    }

    pub fn storage_stats_snapshot(&self) -> StorageStatsSnapshot {
        self.storage.in_mem.stats().snapshot()
    }

    pub fn ts_column_index(&self) -> usize {
        self.ts_column_index
    }

    pub fn batch_pins(&self) -> &Arc<BatchPins> {
        &self.storage.batch_pins
    }

    pub fn start_batch_reclaimer(&self, interval: Duration, batch_budget_per_tick: usize) {
        if self
            .batch_reclaimer_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let retired = self.storage.batch_retirement.clone();
        let store = self.storage.batch_store.clone();
        let pins = self.storage.batch_pins.clone();
        let handle = retired.spawn_reclaimer(
            store,
            pins,
            self.task_id.clone(),
            interval,
            batch_budget_per_tick,
        );
        *self.batch_reclaimer_handle.lock().expect("batch_reclaimer_handle") = Some(handle);
    }

    pub fn start_background_tasks(
        &self,
        compaction_interval: Duration,
        dump_interval: Duration,
    ) {
        // Start the batch reclaimer too (it is cheap and store-agnostic).
        self.start_batch_reclaimer(dump_interval, 1024);

        if self
            .background_tasks_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let compactor = Arc::new(self.storage.compactor.clone());
        let window_states = Arc::new(self.window_states.clone());
        let (compact_handle, dump_handle) = compactor.spawn_background_tasks(
            window_states,
            self.task_id.clone(),
            self.ts_column_index,
            self.dump_hot_bucket_count,
            self.in_mem_dump_parallelism,
            compaction_interval,
            dump_interval,
        );
        *self.compaction_handle.lock().expect("compaction_handle") = Some(compact_handle);
        *self.dump_handle.lock().expect("dump_handle") = Some(dump_handle);
    }

    pub async fn stop_background_tasks(&self) {
        let compact = self.compaction_handle.lock().expect("compaction_handle").take();
        let dump = self.dump_handle.lock().expect("dump_handle").take();
        let reclaim = self
            .batch_reclaimer_handle
            .lock()
            .expect("batch_reclaimer_handle")
            .take();

        if let Some(h) = compact {
            h.abort();
            let _ = h.await;
        }
        if let Some(h) = dump {
            h.abort();
            let _ = h.await;
        }
        if let Some(h) = reclaim {
            h.abort();
            let _ = h.await;
        }

        self.background_tasks_started.store(false, Ordering::Release);
        self.batch_reclaimer_started.store(false, Ordering::Release);
    }

    pub async fn periodic_dump_to_store(&self) -> anyhow::Result<()> {
        self.storage
            .compactor
            .periodic_dump_to_store(
                &self.window_states,
                self.task_id.clone(),
                self.ts_column_index,
                self.dump_hot_bucket_count,
                self.in_mem_dump_parallelism,
            )
            .await
    }

    pub async fn compact_bucket_on_read(
        &self,
        key: &Key,
        bucket_ts: Timestamp,
    ) {
        let Some(arc) = self.get_windows_state_arc(key) else {
            return;
        };
        self.storage
            .compactor
            .compact_bucket(key, &arc, self.task_id.clone(), bucket_ts, self.ts_column_index)
            .await;
    }

    pub async fn rehydrate_bucket(
        &self,
        key: &Key,
        bucket_ts: Timestamp,
    ) {
        let Some(arc) = self.get_windows_state_arc(key) else {
            return;
        };
        self.storage
            .compactor
            .rehydrate_bucket(key, &arc, self.task_id.clone(), bucket_ts)
            .await;
    }

    /// Flush all in-memory runs into the store for checkpointing.
    ///
    /// This makes checkpoints depend only on the store snapshot (plus logical metadata).
    pub async fn flush_in_mem_runs_to_store(&self) -> anyhow::Result<()> {
        self.storage
            .compactor
            .checkpoint_flush_to_store(
                &self.window_states,
                self.task_id.clone(),
                self.ts_column_index,
                self.in_mem_dump_parallelism,
            )
            .await
    }

    /// Checkpoint/batch-finalization durability barrier.
    ///
    /// For remote-backed stores (e.g. SlateDB), this waits for pending writes to be persisted.
    /// For purely in-memory stores, it is a no-op.
    pub async fn await_store_persisted(&self) -> anyhow::Result<()> {
        self.storage.batch_store.await_persisted().await
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
    ) {
        self.window_states.clear();
        self.storage.in_mem.clear();
        self.storage.compactor.clear_dump_state();
        for (key_bytes, windows_state_bytes) in checkpoint.keys {
            let key = Key::from_bytes(&key_bytes);
            let windows_state: WindowsState =
                bincode::deserialize(&windows_state_bytes).expect("Failed to deserialize WindowsState checkpoint");

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

    pub fn get_windows_state_arc(&self, key: &Key) -> Option<Arc<RwLock<WindowsState>>> {
        self.window_states.get(key).map(|e| e.value().clone())
    }

    pub async fn insert_batch(
        &self,
        key: &Key,
        watermark_ts: Option<Timestamp>,
        batch: RecordBatch,
    ) -> usize {
        // Get or create windows_state
        let arc_rwlock = if let Some(entry) = self.window_states.get(key) {
            entry.value().clone()
        } else {
            let windows_state = create_empty_windows_state(
                &self.window_ids,
                &self.tiling_configs,
                self.bucket_granularity,
            );
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
        // - If watermark is known, drop anything with ts <= watermark (already finalized).
        //   (We currently don't support late-event recomputation.)
        // - Otherwise: keep all rows (can't classify lateness yet).
        let (record_batch, dropped_rows) = if let Some(wm) = watermark_ts {
            drop_too_late_entries(&record_batch_with_seq, self.ts_column_index, 0, wm)
        } else {
            (record_batch_with_seq.clone(), 0)
        };

        if record_batch.num_rows() == 0 {
            return dropped_rows;
        }

        // Hot write path: partition into per-bucket batches (sorted by rowpos) and keep them in-memory (no BatchId).
        let batches = self
            .storage
            .batch_store
            .partition_records(&record_batch, key, self.ts_column_index);

        // Calculate pre-aggregated tiles if needed (use already-sorted batches).
        for (window_id, window_state) in windows_state.window_states.iter_mut() {
            if let Some(ref mut tiles) = window_state.tiles {
                let window_expr = &self
                    .window_configs
                    .get(&window_id)
                    .expect("Window config should exist")
                    .window_expr;
                for (_bucket_ts, b) in batches.iter() {
                    tiles.add_sorted_batch(b, window_expr, self.ts_column_index);
                }
            }
        }

        for (bucket_ts, batch) in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let seq_column_index = get_seq_no_column_index(&batch)
                .expect("Expected __seq_no column to exist in hot runs");
            let (min_pos, max_pos) =
                get_batch_rowpos_range(&batch, self.ts_column_index, seq_column_index);
            let row_count = batch.num_rows();
            let bytes_estimate = batch.get_array_memory_size();

            let id = self.storage.in_mem.put(batch);
            windows_state
                .bucket_index
                .insert_batch_ref(bucket_ts, BatchRef::InMem(id), min_pos, max_pos, row_count, bytes_estimate);
        }

        let over_limit = self.storage.in_mem.is_over_limit();
        drop(windows_state); // release per-key lock before doing IO

        if over_limit {
            let _ = self
                .storage
                .compactor
                .relieve_in_mem_pressure(
                    &self.window_states,
                    self.task_id.clone(),
                    self.ts_column_index,
                    self.dump_hot_bucket_count,
                    self.in_mem_low_watermark_per_mille,
                    self.in_mem_dump_parallelism,
                )
                .await;
        }

        // TODO(metrics): report dropped_rows and per-key drops.
        dropped_rows
    }

    pub async fn prune_if_needed(&self, key: &Key) {
        let Some(lateness) = self.lateness else {
            return;
        };
        use crate::storage::index::get_window_length_ms;
        use crate::storage::index::get_window_size_rows;
        use datafusion::logical_expr::WindowFrameUnits;
        
        let arc_rwlock = self.window_states.get(key).expect("Windows state should exist").value().clone();
        let mut windows_state = arc_rwlock.write().await;
        let mut min_cutoff_timestamp = i64::MAX;

        // For each window state, calculate its specific cutoff and prune tiles
        let window_ids: Vec<WindowId> = windows_state.window_states.keys().cloned().collect();
        for window_id in window_ids {
            let window_config = self
                .window_configs
                .get(&window_id)
                .expect("Window config should exist");
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(&window_id).expect("Window state should exist");
            
            let window_end_ts = window_state.processed_until.map(|p| p.ts).unwrap_or(i64::MIN);
            let window_cutoff = match window_frame.units {
                WindowFrameUnits::Rows => {
                    // ROWS windows are count-based (last N rows), so pruning must also be count-based.
                    // We keep enough buckets to cover the last `window_size` rows ending at roughly
                    // `processed_until.ts - lateness`.
                    let window_size = get_window_size_rows(window_frame);
                    if window_size == 0 || window_end_ts == i64::MIN {
                        0
                    } else if windows_state.bucket_index.is_empty() {
                        0
                    } else {
                        let search_ts = window_end_ts.saturating_sub(lateness);
                        let end_bucket_ts = windows_state
                            .bucket_index
                            .bucket_granularity()
                            .start(search_ts);
                        let all_ts = windows_state.bucket_index.bucket_timestamps();
                        let first_ts = all_ts.first().copied().unwrap_or(0);
                        let last_ts = all_ts.last().copied().unwrap_or(first_ts);
                        let within = BucketRange::new(first_ts, end_bucket_ts.min(last_ts));
                        windows_state
                            .bucket_index
                            .plan_rows_tail(search_ts, window_size, within)
                            .unwrap_or(within)
                            .start
                    }
                }
                _ => {
                    // RANGE windows: cutoff = processed_until.ts - window_length - lateness
                    let window_length = get_window_length_ms(window_frame);
                    window_end_ts - window_length - lateness
                }
            };

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
            let pruned = windows_state.bucket_index.prune(min_cutoff_timestamp);

            if !pruned.stored.is_empty() {
                self.storage
                    .batch_retirement
                    .retire(self.task_id.clone(), key, &pruned.stored);
            }
            if !pruned.inmem.is_empty() {
                self.storage.in_mem.remove(&pruned.inmem);
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

    pub fn get_batch_store(&self) -> &Arc<dyn BatchStore> {
        &self.storage.batch_store
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

pub fn create_empty_windows_state(window_ids: &[WindowId], tiling_configs: &[Option<TileConfig>], bucket_granularity: TimeGranularity) -> WindowsState {
    WindowsState {
        window_states: window_ids.iter().map(|&window_id| {
            (window_id, WindowState {
                tiles: tiling_configs.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone()))),
                accumulator_state: None,
                processed_until: None,
            })
        }).collect(),
        bucket_index: BucketIndex::new(bucket_granularity),
        next_seq_no: 0,
    }
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

/// Drop entries that are too late based on a watermark cutoff.
///
/// Drops rows where `ts <= (watermark_ts - lateness_ms)`.
pub fn drop_too_late_entries(
    record_batch: &RecordBatch,
    ts_column_index: usize,
    lateness_ms: i64,
    watermark_ts: Timestamp,
) -> (RecordBatch, usize) {
    use arrow::compute::filter_record_batch;
    use arrow::compute::kernels::cmp::gt;
    use arrow::array::{TimestampMillisecondArray, Scalar};
    
    let ts_column = record_batch.column(ts_column_index);
    
    // Calculate cutoff timestamp: watermark_ts - lateness_ms
    let cutoff_timestamp = watermark_ts - lateness_ms;
    
    let cutoff_array = TimestampMillisecondArray::from_value(cutoff_timestamp, 1);
    let cutoff_scalar = Scalar::new(&cutoff_array);
    
    let keep_mask = gt(ts_column, &cutoff_scalar)
        .expect("Should be able to compare with cutoff timestamp");
    
    if keep_mask.true_count() == record_batch.num_rows() {
        return (record_batch.clone(), 0);
    }

    let kept = filter_record_batch(record_batch, &keep_mask)
        .expect("Should be able to filter record batch");
    let dropped = record_batch.num_rows() - kept.num_rows();
    (kept, dropped)
}


