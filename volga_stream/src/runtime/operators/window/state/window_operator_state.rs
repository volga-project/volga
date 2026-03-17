use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::operators::window::shared::WindowConfig;
use crate::runtime::operators::window::{
    BucketIndex, Cursor, TileConfig, Tiles, SEQ_NO_COLUMN_NAME,
};
use crate::storage::TimeGranularity;
use crate::runtime::state::OperatorState;
use crate::runtime::utils;
use crate::runtime::TaskId;
use crate::storage::batch::Timestamp;
use crate::storage::index::{bucket_index::BatchRef, SortedRangeView};
use crate::storage::maintenance;
use crate::storage::read::plan::{plan_load_from_index, RangesLoadPlan};
use crate::storage::{StorageStatsSnapshot, WorkerStorageRuntime};

mod lifecycle;
mod prune;
mod read_path;
mod state_serde;
mod write_path;

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

pub struct WindowsStateGuard {
    pub(crate) _arc: Arc<RwLock<WindowsState>>,
    pub(crate) _guard: tokio::sync::OwnedRwLockReadGuard<WindowsState>,
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
    pub processed_pos: Option<Cursor>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsState {
    pub window_states: HashMap<WindowId, WindowState>,
    pub bucket_index: BucketIndex,
    pub next_seq_no: u64,
}

impl WindowsState {
    pub fn bucket_index(&self) -> &BucketIndex {
        &self.bucket_index
    }

    pub fn bucket_index_mut(&mut self) -> &mut BucketIndex {
        &mut self.bucket_index
    }
}

impl crate::storage::maintenance::BucketIndexState for WindowsState {
    fn bucket_index(&self) -> &BucketIndex {
        self.bucket_index()
    }

    fn bucket_index_mut(&mut self) -> &mut BucketIndex {
        self.bucket_index_mut()
    }
}

#[derive(Debug)]
pub struct WindowOperatorState {
    storage: Arc<WorkerStorageRuntime>,
    state_handle: crate::storage::write::StateHandle<WindowsState>,
    task_id: TaskId,

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
// - dumping is the only codepath that writes stored runs and publishes `BatchRef::Stored`

const WINDOW_STATE_NAMESPACE: &str = "window_states";

impl WindowOperatorState {
    pub fn new(
        storage: Arc<WorkerStorageRuntime>,
        task_id: TaskId,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        tiling_configs: Vec<Option<TileConfig>>,
        lateness: Option<i64>,
        dump_hot_bucket_count: usize,
        in_mem_low_watermark_per_mille: u32,
        in_mem_dump_parallelism: usize,
    ) -> Self {
        let bucket_granularity = storage.writer().bucket_granularity();
        let window_ids: Vec<WindowId> = window_configs.keys().cloned().collect();
        let serde = state_serde::windows_state_serde();
        let state_handle = storage.state_handle(task_id.clone(), WINDOW_STATE_NAMESPACE, serde);
        Self {
            storage,
            state_handle,
            task_id,
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

    pub fn storage(&self) -> &Arc<WorkerStorageRuntime> {
        &self.storage
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    pub fn window_keys_count(&self) -> usize {
        self.state_handle.len()
    }

    pub fn storage_stats_snapshot(&self) -> StorageStatsSnapshot {
        self.storage.writer().write_buffer_stats_snapshot()
    }

    pub fn ts_column_index(&self) -> usize {
        self.ts_column_index
    }

    #[cfg(test)]
    pub async fn periodic_dump_to_store_for_tests(&self) -> anyhow::Result<()> {
        maintenance::periodic_dump_to_store(
            &self.storage.maintenance,
            self.state_handle.values().as_ref(),
            self.task_id.clone(),
            self.ts_column_index,
            self.dump_hot_bucket_count,
            self.in_mem_dump_parallelism,
        )
        .await
    }

    #[cfg(test)]
    pub async fn compact_bucket_for_tests(&self, key: &Key, bucket_ts: Timestamp) {
        let Some(arc) = self.get_windows_state_arc(key) else {
            return;
        };
        maintenance::compact_bucket(
            &self.storage.maintenance,
            key,
            &arc,
            self.task_id.clone(),
            bucket_ts,
            self.ts_column_index,
        )
        .await;
    }

    /// Flush all in-memory runs into the store for checkpointing.
    ///
    /// This makes checkpoints depend only on the store snapshot (plus logical metadata).
    pub async fn flush_in_mem_runs_to_store(&self) -> anyhow::Result<()> {
        maintenance::checkpoint_flush_to_store(
            &self.storage.maintenance,
            self.state_handle.values().as_ref(),
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
        self.storage.writer().await_store_persisted().await
    }

    pub async fn flush_window_states_to_backend_state(&self) -> anyhow::Result<()> {
        self.state_handle.flush_all().await
    }

    pub async fn restore_window_states_from_backend_state(&self) -> anyhow::Result<()> {
        self.storage.writer().write_buffer_clear();
        self.storage.maintenance.clear_dump_state();
        self.state_handle.restore_all().await
    }

    pub async fn get_windows_state(&self, key: &Key) -> Option<WindowsStateGuard> {
        self.storage
            .writer()
            .record_key_access(self.task_id.clone(), key);
        let arc = self.state_handle.cache().get(key)?;
        let guard = arc.clone().read_owned().await;
        Some(WindowsStateGuard {
            _arc: arc,
            _guard: guard,
        })
    }

    pub fn get_windows_state_arc(&self, key: &Key) -> Option<Arc<RwLock<WindowsState>>> {
        self.storage
            .writer()
            .record_key_access(self.task_id.clone(), key);
        self.state_handle.cache().get(key)
    }

    pub async fn update_window_positions_and_accumulators(
        &self,
        key: &Key,
        new_processed_until: &HashMap<WindowId, Cursor>,
        accumulator_states: &HashMap<WindowId, Option<AccumulatorState>>,
    ) {
        let Some(arc_rwlock) = self.state_handle.cache().get(key) else {
            return;
        };
        let mut windows_state = arc_rwlock.write().await;

        for (window_id, new_pos) in new_processed_until {
            let window_state = windows_state
                .window_states
                .get_mut(window_id)
                .expect("Window state should exist");
            window_state.processed_pos = Some(*new_pos);

            if let Some(accumulator_state) = accumulator_states.get(window_id) {
                window_state.accumulator_state = accumulator_state.clone();
            }
        }

        self.state_handle.cache().mark_dirty(key);
    }

    pub async fn verify_pruning_for_testing(
        &self,
        partition_key: &Key,
        expected_min_timestamp: i64,
    ) {
        let windows_state_ref = self.state_handle.cache().get(partition_key).unwrap();
        let arc_rwlock = windows_state_ref;
        let windows_state = arc_rwlock.read().await;

        // Check batch_index buckets are correctly pruned
        let remaining_bucket_timestamps: Vec<_> = windows_state.bucket_index.bucket_timestamps();

        assert!(
            remaining_bucket_timestamps
                .iter()
                .all(|&ts| ts >= expected_min_timestamp),
            "All bucket timestamps should be >= {}ms, found: {:?}",
            expected_min_timestamp,
            remaining_bucket_timestamps
        );

        // Check window state and tiles
        for (_, window_state) in windows_state.window_states.iter() {
            if let Some(ref tiles) = window_state.tiles {
                let tile_ranges: Vec<_> = tiles.get_tiles_for_range(0, i64::MAX);
                for tile in tile_ranges {
                    assert!(
                        tile.tile_end > expected_min_timestamp,
                        "Tile should end after cutoff {}ms, but tile ends at {}ms",
                        expected_min_timestamp,
                        tile.tile_end
                    );
                }
            }
        }
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

pub fn create_empty_windows_state(
    window_ids: &[WindowId],
    tiling_configs: &[Option<TileConfig>],
    bucket_granularity: TimeGranularity,
) -> WindowsState {
    WindowsState {
        window_states: window_ids
            .iter()
            .map(|&window_id| {
                (
                    window_id,
                    WindowState {
                        tiles: tiling_configs.get(window_id).and_then(|tile_config| {
                            tile_config
                                .as_ref()
                                .map(|config| Tiles::new(config.clone()))
                        }),
                        accumulator_state: None,
                        processed_pos: None,
                    },
                )
            })
            .collect(),
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

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false));
    let schema = Arc::new(Schema::new(fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(seq));

    RecordBatch::try_new(schema, columns).expect("Should be able to append __seq_no column")
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
    use arrow::array::{Scalar, TimestampMillisecondArray};
    use arrow::compute::filter_record_batch;
    use arrow::compute::kernels::cmp::gt;

    let ts_column = record_batch.column(ts_column_index);

    // Calculate cutoff timestamp: watermark_ts - lateness_ms
    let cutoff_timestamp = watermark_ts - lateness_ms;

    let cutoff_array = TimestampMillisecondArray::from_value(cutoff_timestamp, 1);
    let cutoff_scalar = Scalar::new(&cutoff_array);

    let keep_mask =
        gt(ts_column, &cutoff_scalar).expect("Should be able to compare with cutoff timestamp");

    if keep_mask.true_count() == record_batch.num_rows() {
        return (record_batch.clone(), 0);
    }

    let kept = filter_record_batch(record_batch, &keep_mask)
        .expect("Should be able to filter record batch");
    let dropped = record_batch.num_rows() - kept.num_rows();
    (kept, dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use crate::storage::StorageBackendConfig;

    fn make_key(s: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![s]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        Key::new(rb).unwrap()
    }

    fn make_ingest_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts: Vec<i64> = (0..rows as i64).collect();
        let ts_arr = TimestampMillisecondArray::from(ts);
        let val_arr = Float64Array::from(vec![1.0; rows]);
        RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(val_arr)]).unwrap()
    }

    #[tokio::test]
    async fn ingest_triggers_pressure_relief_when_in_mem_over_limit() {
        let mut cfg = StorageBackendConfig::default();
        cfg.memory.total_limit_bytes = 1024 * 1024;
        cfg.memory.write_buffer.reserve_bytes = 0;
        cfg.memory.write_buffer.soft_max_bytes = 1024;
        cfg.pressure.hot_low_watermark_per_mille = 500;
        cfg.concurrency.max_inflight_keys = 1;
        cfg.concurrency.load_io_parallelism = 1;

        let storage = crate::storage::WorkerStorageRuntime::new_with_backend(cfg, move || {
            Arc::new(crate::storage::backend::inmem::InMemBackend::new(
                TimeGranularity::Seconds(1),
                256,
            ))
        })
        .unwrap();

        let state = WindowOperatorState::new(
            storage,
            Arc::<str>::from("t"),
            0, // timestamp column index
            Arc::new(BTreeMap::new()),
            Vec::new(),
            None,
            0, // dump_hot_bucket_count: treat everything as cold for eviction
            cfg.pressure.hot_low_watermark_per_mille,
            4, // in_mem_dump_parallelism
        );

        let stats_before = state.storage_stats_snapshot();

        let key = make_key("A");
        let batch = make_ingest_batch(4096);

        let _ = state.insert_batch(&key, None, batch).await;

        let limit = state.storage().writer().write_buffer_soft_max_bytes();
        let low = (limit.saturating_mul(cfg.pressure.hot_low_watermark_per_mille as usize)) / 1000;
        assert!(
            state.storage().writer().write_buffer_bytes() <= low,
            "expected eviction down to low watermark (<= {}), got {}",
            low,
            state.storage().writer().write_buffer_bytes()
        );

        let stats_after = state.storage_stats_snapshot();
        assert!(
            stats_after.pressure_relief_runs > stats_before.pressure_relief_runs,
            "expected at least one pressure relief run"
        );
        assert!(
            stats_after.pressure_dumped_buckets > stats_before.pressure_dumped_buckets,
            "expected at least one bucket dumped during pressure relief"
        );
    }
}


