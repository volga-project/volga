use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::state::tile::update::{
    apply_batch_to_tiles, plan_update_runs_for_batch,
};
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::storage::WriteBatch;

pub type WindowId = usize;
pub type AccumulatorState = Vec<ScalarValue>;

/// SortedKV-backed window runtime state (WO write path).
#[derive(Debug)]
pub struct WindowOperatorState {
    store: WindowStateStore,
    ts_column_index: usize,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    window_ids: Vec<WindowId>,
    lateness: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOperatorStateCheckpoint {
    pub namespace: Vec<u8>,
}

impl WindowOperatorState {
    pub fn new(
        store: WindowStateStore,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        lateness: Option<i64>,
    ) -> Self {
        let window_ids: Vec<WindowId> = window_configs.keys().cloned().collect();
        Self {
            store,
            ts_column_index,
            window_configs,
            window_ids,
            lateness,
        }
    }

    pub fn store(&self) -> &WindowStateStore {
        &self.store
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.store.flush().await
    }

    pub fn to_checkpoint(&self) -> WindowOperatorStateCheckpoint {
        WindowOperatorStateCheckpoint {
            namespace: self.store.ns.bytes.clone(),
        }
    }

    /// Insert rows; returns `(dropped_count, max_seen after insert)`.
    pub async fn insert_batch(&self, key: &Key, batch: RecordBatch) -> (usize, Option<Cursor>) {
        if batch.num_rows() == 0 {
            let max_seen = self
                .store
                .meta
                .get_key(key)
                .await
                .ok()
                .and_then(|s| s.max_seen);
            return (0, max_seen);
        }

        let mut key_state = self.store.meta.get_key(key).await.expect("key state");
        let processed_pos_ts = key_state
            .processed_pos
            .map(|p| p.ts)
            .unwrap_or(i64::MIN);

        // Streaming late: ts <= processed_pos (no lateness slack). CDC late-data later.
        // Drop before seq assign so next_seq ≡ max_seen.seq + 1.
        let (accepted, dropped) =
            drop_late_entries(&batch, self.ts_column_index, processed_pos_ts);
        if accepted.num_rows() == 0 {
            return (dropped, key_state.max_seen);
        }

        let start_seq = key_state.next_seq();
        let with_seq = append_seq_no_column(&accepted, start_seq);

        if let Some((batch_min, batch_max)) = cursor_range_from_batch(&with_seq, self.ts_column_index)
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

        // Single-writer ingest: plan tile keys → parallel get → update in mem → atomic write.
        let mut tile_keys = std::collections::BTreeSet::new();
        let mut tiling_windows = Vec::new();
        for window_id in &self.window_ids {
            let Some(cfg) = self
                .window_configs
                .get(window_id)
                .and_then(|c| c.tiling.clone())
            else {
                continue;
            };
            for run in plan_update_runs_for_batch(&cfg, &with_seq, self.ts_column_index) {
                tile_keys.insert((run.granularity, run.start_ts));
            }
            tiling_windows.push((*window_id, cfg));
        }
        let mut updated_tiles = BTreeMap::new();
        if !tile_keys.is_empty() {
            let futs: Vec<_> = tile_keys
                .into_iter()
                .map(|(gran, tile_start)| {
                    let store = &self.store;
                    async move {
                        let wt = store.tiles.get(key, gran, tile_start).await?;
                        Ok::<_, anyhow::Error>(((gran, tile_start), wt))
                    }
                })
                .collect();
            let loaded = futures::future::try_join_all(futs)
                .await
                .expect("load tiles");
            updated_tiles.extend(loaded);
        }
        for (window_id, cfg) in &tiling_windows {
            let window_expr = &self
                .window_configs
                .get(window_id)
                .expect("cfg")
                .window_expr;
            apply_batch_to_tiles(
                &mut updated_tiles,
                *window_id,
                cfg,
                window_expr,
                &with_seq,
                self.ts_column_index,
            );
        }

        let mut wb = WriteBatch::new();
        self.store
            .events
            .append_batch_rows(&mut wb, key, &with_seq, self.ts_column_index)
            .expect("append rows");
        self.store
            .tiles
            .append_window_tiles(&mut wb, key, &updated_tiles)
            .expect("append tiles");
        self.store
            .meta
            .append_put_key(&mut wb, key, &key_state)
            .expect("append meta");

        self.store.kv.write(wb).await.expect("atomic ingest write");
        (dropped, key_state.max_seen)
    }

    /// Retention GC only: drop raw/tiles older than `processed − max_wl − lateness`.
    /// Does not affect ingest acceptance (that is `processed_pos` only).
    pub async fn prune_if_needed(&self, key: &Key) {
        let Some(retention) = self.lateness else {
            return;
        };
        let key_state = self.store.meta.get_key(key).await.expect("key state");
        let Some(processed) = key_state.processed_pos else {
            return;
        };

        let mut max_wl = 0i64;
        for cfg in self.window_configs.values() {
            max_wl = max_wl.max(get_window_length_ms(cfg.window_expr.get_window_frame()));
        }
        let cutoff_ts = processed
            .ts
            .saturating_sub(max_wl)
            .saturating_sub(retention.max(0));
        if cutoff_ts <= i64::MIN / 2 {
            return;
        }
        let cutoff = Cursor::new(cutoff_ts, u64::MAX);
        let _ = tokio::join!(
            self.store.events.prune_before(key, cutoff),
            self.store.tiles.prune_before(key, cutoff_ts),
        );
    }
}

fn append_seq_no_column(batch: &RecordBatch, start_seq: u64) -> RecordBatch {
    if batch.schema().field_with_name(SEQ_NO_COLUMN_NAME).is_ok() {
        return batch.clone();
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false)));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = batch.columns().to_vec();
    let seqs: Vec<u64> = (0..batch.num_rows())
        .map(|i| start_seq.saturating_add(i as u64))
        .collect();
    columns.push(Arc::new(UInt64Array::from(seqs)));
    RecordBatch::try_new(schema, columns).expect("append seq")
}

/// Min and max cursors in the batch (`None` if empty).
fn cursor_range_from_batch(
    batch: &RecordBatch,
    ts_column_index: usize,
) -> Option<(Cursor, Cursor)> {
    if batch.num_rows() == 0 {
        return None;
    }
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let seq_idx = batch.schema().index_of(SEQ_NO_COLUMN_NAME).ok()?;
    let seq = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("seq");
    let mut min = Cursor::new(ts.value(0), seq.value(0));
    let mut max = min;
    for i in 1..batch.num_rows() {
        let c = Cursor::new(ts.value(i), seq.value(i));
        if c < min {
            min = c;
        }
        if c > max {
            max = c;
        }
    }
    Some((min, max))
}

/// Drop rows with `ts <= processed_pos_ts` (streaming late; no retention slack).
fn drop_late_entries(
    batch: &RecordBatch,
    ts_column_index: usize,
    processed_pos_ts: i64,
) -> (RecordBatch, usize) {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let mut keep = Vec::new();
    for i in 0..batch.num_rows() {
        if ts.value(i) > processed_pos_ts {
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
