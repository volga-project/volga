use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow::array::{RecordBatch, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::model::{WindowId, WindowTrigger, WindowTriggerKind};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::operators::window::store::{
    PartitionKey, StateNamespace, WindowBackendSnapshot, WindowOperatorStore,
};
use crate::runtime::operators::window::tile::{apply_batch_to_tiles, plan_update_runs_for_batch};
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::runtime::operators::OperatorKind;
use crate::runtime::state::OperatorTaskState;

/// Sentinel in [`WindowOperatorState::watermark_frontier`]: no frontier yet.
pub const WATERMARK_UNSET: i64 = i64::MIN;

/// Runtime state owned by the WO (per task / state namespace).
#[derive(Debug)]
pub struct WindowOperatorState {
    store: Arc<dyn WindowOperatorStore>,
    namespace: StateNamespace,
    ts_column_index: usize,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    lateness_ms: i64,
    max_window_length_ms: i64,
    /// Task watermark frontier; [`WATERMARK_UNSET`] until the first advance.
    pub watermark_frontier: AtomicI64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowStateSnapshot {
    pub namespace: Vec<u8>,
    pub watermark_frontier: Option<i64>,
    pub backend: WindowBackendSnapshot,
}

impl WindowOperatorState {
    pub fn new(
        store: Arc<dyn WindowOperatorStore>,
        namespace: StateNamespace,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        lateness_ms: i64,
        max_window_length_ms: i64,
    ) -> Self {
        Self {
            store,
            namespace,
            ts_column_index,
            window_configs,
            lateness_ms,
            max_window_length_ms,
            watermark_frontier: AtomicI64::new(WATERMARK_UNSET),
        }
    }

    pub fn store(&self) -> &dyn WindowOperatorStore {
        self.store.as_ref()
    }

    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    pub fn watermark_frontier(&self) -> Option<i64> {
        let v = self.watermark_frontier.load(Ordering::Acquire);
        (v != WATERMARK_UNSET).then_some(v)
    }

    /// `(watermark, data_floor)` when a frontier has been established.
    ///
    /// Data floor is `W - max_window_length - lateness` (both clamped to ≥ 0).
    /// Raw rows with `ts < floor` and tiles fully below the floor may be pruned.
    /// Consumed triggers (`fire_at.ts <= W`) are dropped separately.
    pub fn retention_cutoff(&self) -> Option<(i64, i64)> {
        let watermark = self.watermark_frontier()?;
        let lateness_ms = self.lateness_ms.max(0);
        let max_window_length_ms = self.max_window_length_ms.max(0);
        let floor = watermark
            .saturating_sub(max_window_length_ms)
            .saturating_sub(lateness_ms);
        Some((watermark, floor))
    }

    pub fn partition(&self, key: &Key) -> PartitionKey {
        PartitionKey::new(&self.namespace, key)
    }

    pub async fn checkpoint(&self) -> anyhow::Result<WindowStateSnapshot> {
        Ok(WindowStateSnapshot {
            namespace: self.namespace.bytes.clone(),
            watermark_frontier: self.watermark_frontier(),
            backend: self.store.checkpoint(&self.namespace).await?,
        })
    }

    pub async fn restore(&self, restore: WindowStateSnapshot) -> anyhow::Result<()> {
        anyhow::ensure!(
            restore.namespace == self.namespace.bytes,
            "window checkpoint namespace does not match runtime namespace",
        );
        self.store.restore(&self.namespace, &restore.backend).await?;
        self.watermark_frontier.store(
            restore.watermark_frontier.unwrap_or(WATERMARK_UNSET),
            Ordering::Release,
        );
        Ok(())
    }

    pub async fn insert_batch(
        &self,
        key: &Key,
        batch: RecordBatch,
        schedule_row_triggers: bool,
    ) -> usize {
        if batch.num_rows() == 0 {
            return 0;
        }
        let partition = self.partition(key);
        let watermark_frontier = self.watermark_frontier();

        let mut key_state = self
            .store
            .load_key_state(&partition)
            .await
            .expect("key state");
        let (accepted, dropped) =
            drop_late_entries(&batch, self.ts_column_index, watermark_frontier);
        if accepted.num_rows() == 0 {
            return dropped;
        }

        let start_seq = key_state.next_seq;
        let with_seq = append_seq_no_column(&accepted, start_seq);
        key_state.next_seq = start_seq
            .checked_add(accepted.num_rows() as u64)
            .expect("per-key sequence exhausted");

        let mut tile_runs = Vec::new();
        let mut tiling_windows = Vec::new();
        for (window_id, window) in self.window_configs.iter() {
            let Some(cfg) = window.tiling.clone() else {
                continue;
            };
            for run in plan_update_runs_for_batch(&cfg, &with_seq, self.ts_column_index) {
                tile_runs.push(run);
            }
            tiling_windows.push((*window_id, cfg, Arc::clone(&window.window_expr)));
        }
        tile_runs.sort_by_key(|run| (run.granularity, run.start_ts, run.end_ts_exclusive));
        tile_runs.dedup();
        let mut updated_tiles = self
            .store
            .load_tiles(&partition, &tile_runs)
            .await
            .expect("load tiles");
        for run in &tile_runs {
            updated_tiles
                .entry((run.granularity, run.start_ts))
                .or_default();
        }
        for (window_id, cfg, window_expr) in &tiling_windows {
            apply_batch_to_tiles(
                &mut updated_tiles,
                *window_id,
                cfg,
                window_expr,
                &with_seq,
                self.ts_column_index,
            );
        }

        let triggers = if schedule_row_triggers {
            cursors_from_batch(&with_seq, self.ts_column_index)
                .expect("stored batch cursor columns")
                .into_iter()
                .map(|fire_at| WindowTrigger {
                    fire_at,
                    partition: partition.clone(),
                    kind: WindowTriggerKind::RowEmit,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        self.store
            .commit_events(
                &partition,
                self.ts_column_index,
                &with_seq,
                &updated_tiles,
                &key_state,
                &triggers,
            )
            .await
            .expect("atomic ingest write");
        dropped
    }
}

impl OperatorTaskState for WindowOperatorState {
    fn state_namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    fn kind(&self) -> OperatorKind {
        OperatorKind::Window
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn append_seq_no_column(batch: &RecordBatch, start_seq: u64) -> RecordBatch {
    assert!(
        batch.schema().field_with_name(SEQ_NO_COLUMN_NAME).is_err(),
        "WO ingest assigns __seq_no; input must not already have it",
    );
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        SEQ_NO_COLUMN_NAME,
        DataType::UInt64,
        false,
    )));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = batch.columns().to_vec();
    let seqs: Vec<u64> = (0..batch.num_rows())
        .map(|i| {
            start_seq
                .checked_add(i as u64)
                .expect("per-key sequence exhausted")
        })
        .collect();
    columns.push(Arc::new(UInt64Array::from(seqs)));
    RecordBatch::try_new(schema, columns).expect("append seq")
}

/// Drop rows at or behind the task watermark.
fn drop_late_entries(
    batch: &RecordBatch,
    ts_column_index: usize,
    watermark_frontier: Option<i64>,
) -> (RecordBatch, usize) {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let mut keep = Vec::new();
    for i in 0..batch.num_rows() {
        if watermark_frontier.map_or(true, |watermark| ts.value(i) > watermark) {
            keep.push(i as u32);
        }
    }
    let dropped = batch.num_rows() - keep.len();
    if keep.len() == batch.num_rows() {
        return (batch.clone(), 0);
    }
    if keep.is_empty() {
        return (RecordBatch::new_empty(batch.schema()), dropped);
    }
    let indices = arrow::array::UInt32Array::from(keep);
    let kept = arrow::compute::take_record_batch(batch, &indices).expect("take");
    (kept, dropped)
}
