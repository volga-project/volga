use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::model::{Cursor, WindowId};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::operators::window::store::{
    PartitionKey, StateNamespace, WindowBackendSnapshot, WindowOperatorStore,
};
use crate::runtime::operators::window::tile::{apply_batch_to_tiles, plan_update_runs_for_batch};
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;

/// Runtime state owned by the WO.
#[derive(Debug)]
pub struct WindowOperatorState {
    store: Arc<dyn WindowOperatorStore>,
    namespace: StateNamespace,
    ts_column_index: usize,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowStateSnapshot {
    pub namespace: Vec<u8>,
    pub backend: WindowBackendSnapshot,
}

impl WindowOperatorState {
    pub fn new(
        store: Arc<dyn WindowOperatorStore>,
        namespace: StateNamespace,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    ) -> Self {
        Self {
            store,
            namespace,
            ts_column_index,
            window_configs,
        }
    }

    pub fn store(&self) -> &dyn WindowOperatorStore {
        self.store.as_ref()
    }

    pub fn partition(&self, key: &Key) -> PartitionKey {
        PartitionKey::new(&self.namespace, key)
    }

    pub async fn checkpoint(&self) -> anyhow::Result<WindowStateSnapshot> {
        Ok(WindowStateSnapshot {
            namespace: self.namespace.bytes.clone(),
            backend: self.store.checkpoint(&self.namespace).await?,
        })
    }

    pub async fn restore(&self, restore: WindowStateSnapshot) -> anyhow::Result<()> {
        anyhow::ensure!(
            restore.namespace == self.namespace.bytes,
            "window checkpoint namespace does not match runtime namespace",
        );
        self.store.restore(&self.namespace, &restore.backend).await
    }

    /// Insert rows; returns `(dropped_count, max_seen after insert)`.
    pub async fn insert_batch(&self, key: &Key, batch: RecordBatch) -> (usize, Option<Cursor>) {
        if batch.num_rows() == 0 {
            return (0, None);
        }
        let partition = self.partition(key);

        let mut key_state = self.store.load_meta(&partition).await.expect("key state");
        // Drop before seq assign so `next_seq` stays a dense allocator.
        let (accepted, dropped) = drop_late_entries(
            &batch,
            self.ts_column_index,
            key_state.processed_pos,
            key_state.next_seq,
        );
        if accepted.num_rows() == 0 {
            return (dropped, key_state.max_seen);
        }

        let start_seq = key_state.next_seq;
        let with_seq = append_seq_no_column(&accepted, start_seq);
        // Dedicated allocator (not max_seen.seq — that is ts-first cursor order).
        key_state.next_seq = start_seq
            .checked_add(accepted.num_rows() as u64)
            .expect("per-key sequence exhausted");

        if let Some((batch_min, batch_max)) =
            cursor_range_from_batch(&with_seq, self.ts_column_index)
        {
            key_state.max_seen = Some(match key_state.max_seen {
                Some(prev) => prev.max(batch_max),
                None => batch_max,
            });
            key_state.first_ingested = Some(match key_state.first_ingested {
                Some(prev) => prev.min(batch_min),
                None => batch_min,
            });
        }

        // Load and update every tile touched by this batch.
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

        self.store
            .commit_events(
                &partition,
                self.ts_column_index,
                &with_seq,
                &updated_tiles,
                &key_state,
            )
            .await
            .expect("atomic ingest write");
        (dropped, key_state.max_seen)
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

/// Min and max cursors in the batch (`None` if empty).
fn cursor_range_from_batch(
    batch: &RecordBatch,
    ts_column_index: usize,
) -> Option<(Cursor, Cursor)> {
    let cursors = cursors_from_batch(batch, ts_column_index).ok()?;
    let min = cursors.iter().copied().min()?;
    let max = cursors.iter().copied().max()?;
    Some((min, max))
}

/// Drop rows at or behind the processed cursor.
fn drop_late_entries(
    batch: &RecordBatch,
    ts_column_index: usize,
    processed_pos: Option<Cursor>,
    next_seq: u64,
) -> (RecordBatch, usize) {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let mut keep = Vec::new();
    for i in 0..batch.num_rows() {
        let seq_no = next_seq
            .checked_add(keep.len() as u64)
            .expect("per-key sequence exhausted");
        let cursor = Cursor::new(ts.value(i), seq_no);
        if processed_pos.map_or(true, |processed| cursor > processed) {
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
