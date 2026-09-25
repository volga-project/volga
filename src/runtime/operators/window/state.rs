use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{RecordBatch, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::observability::snapshot_types::TaskOperatorMetrics;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::metrics::collect_window_operator_snapshot;
use crate::runtime::operators::window::model::{WindowId, WindowTrigger, WindowTriggerKind};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::operators::window::store::{
    PartitionKey, StateNamespace, WindowBackendSnapshot, WindowOperatorStore, WindowStoreTaskScope,
};
use crate::runtime::operators::window::tile::{apply_batch_to_tiles, plan_update_runs_for_batch};
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::runtime::operators::OperatorKind;
use crate::runtime::state::OperatorTaskState;
use crate::runtime::VertexId;
use async_trait::async_trait;

/// Sentinel in [`WindowOperatorState::watermark_frontier`]: no frontier yet.
pub const WATERMARK_UNSET: i64 = i64::MIN;

/// Runtime state owned by the WO (per task / state namespace).
#[derive(Debug)]
pub struct WindowOperatorState {
    store: Arc<dyn WindowOperatorStore>,
    scope: WindowStoreTaskScope,
    task_id: VertexId,
    ts_column_index: usize,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    lateness_ms: i64,
    max_window_length_ms: i64,
    /// Task watermark frontier; [`WATERMARK_UNSET`] until the first advance.
    pub watermark_frontier: AtomicI64,
    /// Watermark of the last **completed** checkpoint. GC reads this, never the live frontier.
    committed_wm: AtomicI64,
    /// Highest checkpoint id applied by `notify_checkpoint_complete`.
    applied_checkpoint_id: AtomicU64,
    /// `StateOnly` admits `ts >=` the committed retention floor, not the live watermark.
    state_only: bool,
    /// Cuts captured at each barrier, published on completion (#300).
    pending_checkpoints: Mutex<HashMap<u64, WindowStateSnapshot>>,
    /// Docker maintain has no `WindowExpr`. Empty means read `window_configs`.
    #[cfg(test)]
    tile_granularities_ms: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowStateSnapshot {
    pub namespace: Vec<u8>,
    pub watermark_frontier: Option<i64>,
    #[serde(default)]
    pub checkpoint_id: Option<u64>,
    pub backend: WindowBackendSnapshot,
}

impl WindowOperatorState {
    pub fn new(
        store: Arc<dyn WindowOperatorStore>,
        task_id: VertexId,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        lateness_ms: i64,
        max_window_length_ms: i64,
        scope: WindowStoreTaskScope,
    ) -> Self {
        Self {
            store,
            scope,
            task_id,
            ts_column_index,
            window_configs,
            lateness_ms,
            max_window_length_ms,
            watermark_frontier: AtomicI64::new(WATERMARK_UNSET),
            committed_wm: AtomicI64::new(WATERMARK_UNSET),
            applied_checkpoint_id: AtomicU64::new(0),
            state_only: false,
            pending_checkpoints: Mutex::new(HashMap::new()),
            #[cfg(test)]
            tile_granularities_ms: Vec::new(),
        }
    }

    pub fn with_state_only(mut self, state_only: bool) -> Self {
        self.state_only = state_only;
        self
    }

    #[cfg(test)]
    pub fn for_test(
        store: Arc<dyn WindowOperatorStore>,
        namespace: StateNamespace,
        task_id: VertexId,
        ts_column_index: usize,
        window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
        lateness_ms: i64,
        max_window_length_ms: i64,
    ) -> Self {
        Self::new(
            store,
            task_id,
            ts_column_index,
            window_configs,
            lateness_ms,
            max_window_length_ms,
            WindowStoreTaskScope::for_test(namespace),
        )
    }

    pub fn store(&self) -> &dyn WindowOperatorStore {
        self.store.as_ref()
    }

    pub fn namespace(&self) -> &StateNamespace {
        &self.scope.namespace
    }

    pub fn scope(&self) -> &WindowStoreTaskScope {
        &self.scope
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
        self.cutoff_at(self.watermark_frontier())
    }

    /// Floor for physical delete: last completed checkpoint, not the live watermark.
    pub fn committed_retention_cutoff(&self) -> Option<(i64, i64)> {
        self.cutoff_at(self.committed_watermark())
    }

    pub fn committed_watermark(&self) -> Option<i64> {
        let v = self.committed_wm.load(Ordering::Acquire);
        (v != WATERMARK_UNSET).then_some(v)
    }

    fn cutoff_at(&self, watermark: Option<i64>) -> Option<(i64, i64)> {
        let watermark = watermark?;
        Some((watermark, self.retention_floor_at(Some(watermark))?))
    }

    pub fn tile_granularity_ms(&self) -> Vec<i64> {
        #[cfg(test)]
        if !self.tile_granularities_ms.is_empty() {
            return self.tile_granularities_ms.clone();
        }
        let mut seen = std::collections::BTreeSet::new();
        for window in self.window_configs.values() {
            if let Some(tiling) = &window.tiling {
                for g in &tiling.granularities {
                    seen.insert(g.to_millis());
                }
            }
        }
        seen.into_iter().collect()
    }

    #[cfg(test)]
    pub fn seed_committed_watermark(&self, watermark: i64) {
        self.committed_wm.store(watermark, Ordering::Release);
    }

    #[cfg(test)]
    pub fn with_tile_granularities(mut self, granularities_ms: Vec<i64>) -> Self {
        self.tile_granularities_ms = granularities_ms;
        self
    }

    pub fn partition(&self, key: &Key) -> PartitionKey {
        PartitionKey::new(&self.scope.namespace, key)
    }

    fn retention_floor_at(&self, watermark: Option<i64>) -> Option<i64> {
        watermark.map(|w| {
            w.saturating_sub(self.max_window_length_ms.max(0))
                .saturating_sub(self.lateness_ms.max(0))
        })
    }

    pub async fn checkpoint(&self, checkpoint_id: u64) -> anyhow::Result<WindowStateSnapshot> {
        let snap = WindowStateSnapshot {
            namespace: self.scope.namespace.bytes.clone(),
            watermark_frontier: self.watermark_frontier(),
            checkpoint_id: Some(checkpoint_id),
            backend: self.store.checkpoint().await?,
        };
        self.pending_checkpoints
            .lock()
            .expect("pending_checkpoints")
            .insert(checkpoint_id, snap.clone());
        Ok(snap)
    }

    pub async fn restore(&self, restore: WindowStateSnapshot) -> anyhow::Result<()> {
        anyhow::ensure!(
            restore.namespace == self.scope.namespace.bytes,
            "window checkpoint namespace does not match runtime namespace",
        );
        self.store.restore(&restore.backend).await?;
        let restored_wm = restore.watermark_frontier.unwrap_or(WATERMARK_UNSET);
        self.watermark_frontier
            .store(restored_wm, Ordering::Release);
        self.committed_wm.store(restored_wm, Ordering::Release);
        if let Some(id) = restore.checkpoint_id {
            let applied = self.applied_checkpoint_id.load(Ordering::Acquire);
            if id > applied {
                self.applied_checkpoint_id.store(id, Ordering::Release);
            }
        }
        Ok(())
    }

    pub async fn notify_checkpoint_complete(&self, checkpoint_id: u64) -> anyhow::Result<()> {
        let snap = self
            .pending_checkpoints
            .lock()
            .expect("pending_checkpoints")
            .get(&checkpoint_id)
            .cloned();
        let Some(snap) = snap else {
            return Ok(());
        };
        // A later checkpoint may share this watermark and still move the cut.
        // An older id must not rewind either one.
        let applied = self.applied_checkpoint_id.load(Ordering::Acquire);
        if checkpoint_id <= applied {
            self.pending_checkpoints
                .lock()
                .expect("pending_checkpoints")
                .remove(&checkpoint_id);
            return Ok(());
        }
        if let Some(wm) = snap.watermark_frontier {
            let current = self.committed_wm.load(Ordering::Acquire);
            if current == WATERMARK_UNSET || wm >= current {
                self.committed_wm.store(wm, Ordering::Release);
            }
        }
        self.store
            .on_checkpoint_complete(
                checkpoint_id,
                &snap.backend,
                snap.watermark_frontier,
                self.retention_floor_at(snap.watermark_frontier),
            )
            .await?;
        self.applied_checkpoint_id
            .store(checkpoint_id, Ordering::Release);
        self.pending_checkpoints
            .lock()
            .expect("pending_checkpoints")
            .remove(&checkpoint_id);
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
        let cutoff = if self.state_only {
            self.retention_floor_at(self.committed_watermark())
        } else {
            self.watermark_frontier()
        };

        let (accepted, dropped) =
            drop_late_entries(&batch, self.ts_column_index, cutoff, self.state_only);
        if accepted.num_rows() == 0 {
            return dropped;
        }

        let mut tile_runs = Vec::new();
        let mut tiling_windows = Vec::new();
        for (window_id, window) in self.window_configs.iter() {
            let Some(cfg) = window.tiling.clone() else {
                continue;
            };
            for run in plan_update_runs_for_batch(&cfg, &accepted, self.ts_column_index) {
                tile_runs.push(run);
            }
            tiling_windows.push((*window_id, cfg, Arc::clone(&window.window_expr)));
        }
        tile_runs.sort_by_key(|run| (run.granularity, run.start_ts, run.end_ts_exclusive));
        tile_runs.dedup();

        let (mut key_state, mut updated_tiles) = tokio::try_join!(
            self.store.load_key_state(&partition),
            self.store.load_tiles(&partition, &tile_runs),
        )
        .expect("key state and tiles");

        let start_seq = key_state.next_seq;
        let with_seq = append_seq_no_column(&accepted, start_seq);
        key_state.next_seq = start_seq
            .checked_add(accepted.num_rows() as u64)
            .expect("per-key sequence exhausted");
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

#[async_trait]
impl OperatorTaskState for WindowOperatorState {
    fn state_namespace(&self) -> &StateNamespace {
        &self.scope.namespace
    }

    fn kind(&self) -> OperatorKind {
        OperatorKind::Window
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn task_id(&self) -> &str {
        self.task_id.as_ref()
    }

    async fn task_operator_metrics(&self) -> Option<TaskOperatorMetrics> {
        Some(TaskOperatorMetrics::Window(
            collect_window_operator_snapshot(self.task_id(), self.store.metrics_labels()),
        ))
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

/// Emit drops `ts <= watermark`. StateOnly drops `ts <` the committed floor
/// (`None` admits all), matching GC which keeps `ts >= floor`.
fn drop_late_entries(
    batch: &RecordBatch,
    ts_column_index: usize,
    cutoff: Option<i64>,
    inclusive: bool,
) -> (RecordBatch, usize) {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let mut keep = Vec::new();
    for i in 0..batch.num_rows() {
        let admit = cutoff.map_or(true, |floor| {
            if inclusive {
                ts.value(i) >= floor
            } else {
                ts.value(i) > floor
            }
        });
        if admit {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::runtime::operators::window::model::StateNamespace;
    use crate::runtime::operators::window::store::backend::InMemWindowStore;

    fn state_at(ns: &StateNamespace, watermark: i64) -> WindowOperatorState {
        let store = InMemWindowStore::new();
        let state = WindowOperatorState::for_test(
            Arc::new(store.client(WindowStoreTaskScope::for_test(ns.clone()))),
            ns.clone(),
            Arc::from("notify"),
            0,
            Arc::new(BTreeMap::new()),
            0,
            0,
        );
        state.watermark_frontier.store(watermark, Ordering::Release);
        state
    }

    #[tokio::test]
    async fn older_notify_does_not_rewind_a_newer_checkpoint() {
        let ns = StateNamespace::new(b"op");
        let state = state_at(&ns, 5_000);
        state.checkpoint(1).await.unwrap();
        state.notify_checkpoint_complete(1).await.unwrap();
        state.watermark_frontier.store(5_000, Ordering::Release);
        state.checkpoint(2).await.unwrap();
        state.notify_checkpoint_complete(2).await.unwrap();
        assert_eq!(state.committed_watermark(), Some(5_000));
        assert_eq!(state.applied_checkpoint_id.load(Ordering::Acquire), 2);

        state.watermark_frontier.store(10_000, Ordering::Release);
        state.checkpoint(3).await.unwrap();
        state.notify_checkpoint_complete(3).await.unwrap();
        state.notify_checkpoint_complete(1).await.unwrap();
        state.notify_checkpoint_complete(2).await.unwrap();
        assert_eq!(state.committed_watermark(), Some(10_000));
        assert_eq!(state.applied_checkpoint_id.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn restore_ignores_an_older_notify() {
        let ns = StateNamespace::new(b"op");
        let state = state_at(&ns, 10_000);
        let snap = state.checkpoint(6).await.unwrap();
        let restored = state_at(&ns, 0);
        restored.restore(snap).await.unwrap();
        assert_eq!(restored.committed_watermark(), Some(10_000));

        restored.watermark_frontier.store(1_000, Ordering::Release);
        restored.checkpoint(5).await.unwrap();
        restored.notify_checkpoint_complete(5).await.unwrap();
        assert_eq!(restored.committed_watermark(), Some(10_000));
        assert_eq!(restored.applied_checkpoint_id.load(Ordering::Acquire), 6);
    }
}
