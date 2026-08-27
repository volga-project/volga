//! In-memory window store implementation and behavior tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor as IoCursor;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::concat_batches;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use std::any::Any;

use crate::runtime::metrics::MetricsLabels;
use crate::runtime::operators::window::metrics;
use crate::runtime::operators::window::model::{
    Cursor, RawRun, TileRun, WindowTrigger,
};
use crate::runtime::operators::window::state::WindowOperatorState;
use crate::runtime::state::{OperatorStore, OperatorTaskState};

use super::{
    DueWindowWork, DueWorkStream, WindowBackendSnapshot, WindowOperatorStore, WindowRequestStore,
};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::operators::window::store::{
    KeyState, PartitionKey, StateNamespace, TileMap, WindowData,
};

const MAX_INLINE_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

fn validate_checkpoint_size(snapshot: &[u8]) -> Result<()> {
    anyhow::ensure!(
        snapshot.len() <= MAX_INLINE_CHECKPOINT_BYTES,
        "in-memory window checkpoint is {} bytes, exceeding the development/test inline limit of {} bytes; use a durable state backend for larger state",
        snapshot.len(),
        MAX_INLINE_CHECKPOINT_BYTES,
    );
    Ok(())
}

/// Cursor-ordered rows for one partition, stored as one packed Arrow batch.
#[derive(Debug, Clone, Default)]
struct PackedRaw {
    batch: Option<RecordBatch>,
    /// `cursor → row index` in `batch`. Iteration order is cursor order.
    index: BTreeMap<Cursor, u32>,
}

impl PackedRaw {
    fn len(&self) -> usize {
        self.index.len()
    }

    fn memory_size(&self) -> usize {
        self.batch
            .as_ref()
            .map(|batch| batch.get_array_memory_size())
            .unwrap_or(0)
    }

    fn insert(&mut self, events: &RecordBatch, ts_column_index: usize) -> Result<()> {
        if events.num_rows() == 0 {
            return Ok(());
        }
        let new_cursors = cursors_from_batch(events, ts_column_index)?;
        if self.batch.is_none() {
            *self = Self::pack(events, &new_cursors)?;
            return Ok(());
        }
        if self.can_append(&new_cursors) {
            return self.append(events, &new_cursors);
        }
        let existing = self.batch.as_ref().expect("non-empty packed raw");
        let merged = concat_batches(&existing.schema(), [existing, events])?;
        *self = Self::from_batch(&merged, ts_column_index)?;
        Ok(())
    }

    fn can_append(&self, new_cursors: &[Cursor]) -> bool {
        let Some((max, _)) = self.index.last_key_value() else {
            return true;
        };
        new_cursors.windows(2).all(|pair| pair[0] < pair[1])
            && new_cursors.first().is_some_and(|cursor| cursor > max)
    }

    fn append(&mut self, events: &RecordBatch, new_cursors: &[Cursor]) -> Result<()> {
        let existing = self.batch.as_ref().expect("non-empty packed raw");
        let merged = concat_batches(&existing.schema(), [existing, events])?;
        let base = self.index.len() as u32;
        for (i, cursor) in new_cursors.iter().enumerate() {
            self.index.insert(*cursor, base + i as u32);
        }
        self.batch = Some(merged);
        Ok(())
    }

    fn from_batch(batch: &RecordBatch, ts_column_index: usize) -> Result<Self> {
        let cursors = cursors_from_batch(batch, ts_column_index)?;
        Self::pack(batch, &cursors)
    }

    fn pack(batch: &RecordBatch, cursors: &[Cursor]) -> Result<Self> {
        if batch.num_rows() == 0 {
            return Ok(Self::default());
        }
        let mut last_row = BTreeMap::new();
        for (i, cursor) in cursors.iter().enumerate() {
            last_row.insert(*cursor, i as u32);
        }
        let indices: Vec<u32> = last_row.values().copied().collect();
        let already_sorted_unique = indices.len() == batch.num_rows()
            && indices
                .iter()
                .enumerate()
                .all(|(i, &row)| row == i as u32);
        let packed = if already_sorted_unique {
            batch.clone()
        } else {
            let idx = UInt32Array::from(indices);
            arrow::compute::take_record_batch(batch, &idx)?
        };
        let index = last_row
            .into_keys()
            .enumerate()
            .map(|(i, cursor)| (cursor, i as u32))
            .collect();
        Ok(Self {
            batch: Some(packed),
            index,
        })
    }

    fn select(&self, runs: &[RawRun]) -> Vec<RecordBatch> {
        let Some(batch) = &self.batch else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        for run in runs {
            for (_, &row) in self.index.range(run.from..run.to) {
                rows.push(row);
            }
        }
        rows.sort_unstable();
        rows.dedup();
        if rows.is_empty() {
            return Vec::new();
        }
        let mut slices = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let mut end = start + 1;
            while end < rows.len() && rows[end] == rows[end - 1] + 1 {
                end += 1;
            }
            slices.push(batch.slice(rows[start] as usize, end - start));
            start = end;
        }
        if slices.len() == 1 {
            return slices;
        }
        match concat_batches(&batch.schema(), &slices) {
            Ok(concat) => vec![concat],
            Err(_) => slices,
        }
    }

    fn prune_before(&mut self, floor: i64) -> u64 {
        let before = self.index.len();
        self.index.retain(|cursor, _| cursor.ts >= floor);
        let pruned = (before - self.index.len()) as u64;
        if pruned == 0 {
            return 0;
        }
        if self.index.is_empty() {
            self.batch = None;
            return pruned;
        }
        let indices: Vec<u32> = self.index.values().copied().collect();
        let batch = self.batch.as_ref().expect("rows remain after prune");
        let idx = UInt32Array::from(indices);
        let packed = arrow::compute::take_record_batch(batch, &idx)
            .expect("take remaining raw rows");
        self.batch = Some(packed);
        self.index = self
            .index
            .keys()
            .copied()
            .enumerate()
            .map(|(i, cursor)| (cursor, i as u32))
            .collect();
        pruned
    }
}

#[derive(Debug, Clone, Default)]
struct PartitionState {
    meta: KeyState,
    raw: PackedRaw,
    tiles: TileMap,
    ts_column_index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct InMemCheckpoint {
    namespace: Vec<u8>,
    partitions: HashMap<PartitionKey, PartitionCheckpoint>,
    triggers: Vec<WindowTrigger>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartitionCheckpoint {
    meta: KeyState,
    /// One Arrow IPC payload for the packed raw batch; `None` if the partition has no rows.
    raw: Option<Vec<u8>>,
    ts_column_index: usize,
    tiles: TileMap,
}

#[derive(Debug, Default)]
struct InMemInner {
    /// Shared for mutating ops; exclusive for checkpoint / restore / maintain.
    /// One gate per namespace so a Kind soak task does not freeze other tasks.
    namespace_gates: DashMap<Vec<u8>, Arc<RwLock<()>>>,
    partitions: DashMap<PartitionKey, Arc<RwLock<PartitionState>>>,
    triggers: DashMap<Vec<u8>, Arc<RwLock<BTreeSet<WindowTrigger>>>>,
}

/// Development/test reference backend with inline checkpoints limited to 8 MiB.
///
/// Layout: one packed Arrow batch per partition, indexed by cursor. Locking is
/// per-partition (`DashMap` + `RwLock`) plus a per-namespace trigger set. Key
/// eval CPU does not hold any store lock: `load_raw` / `load_tiles` clone under
/// the partition lock and return.
///
/// Snapshot protocol:
/// - WRO `load_window_data` is one partition read lock (raw + tiles together).
/// - Checkpoint / restore / maintain take the namespace gate exclusively so
///   writers cannot tear the clone. Physical prune runs via
///   [`OperatorStore::maintain`], not the WO watermark hot path.
#[derive(Debug, Clone, Default)]
pub struct InMemWindowStore {
    inner: Arc<InMemInner>,
    metrics_labels: Option<MetricsLabels>,
}

impl InMemWindowStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_metrics_labels(mut self, labels: Option<MetricsLabels>) -> Self {
        self.metrics_labels = labels;
        self
    }

    fn namespace_gate(&self, namespace: &[u8]) -> Arc<RwLock<()>> {
        self.inner
            .namespace_gates
            .entry(namespace.to_vec())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    fn partition_slot(&self, partition: &PartitionKey) -> Arc<RwLock<PartitionState>> {
        self.inner
            .partitions
            .entry(partition.clone())
            .or_insert_with(|| Arc::new(RwLock::new(PartitionState::default())))
            .clone()
    }

    fn try_partition(&self, partition: &PartitionKey) -> Option<Arc<RwLock<PartitionState>>> {
        self.inner.partitions.get(partition).map(|entry| entry.clone())
    }

    fn trigger_slot(&self, namespace: &[u8]) -> Arc<RwLock<BTreeSet<WindowTrigger>>> {
        self.inner
            .triggers
            .entry(namespace.to_vec())
            .or_insert_with(|| Arc::new(RwLock::new(BTreeSet::new())))
            .clone()
    }

    fn try_triggers(&self, namespace: &[u8]) -> Option<Arc<RwLock<BTreeSet<WindowTrigger>>>> {
        self.inner.triggers.get(namespace).map(|entry| entry.clone())
    }

    /// Scan namespace store footprint and write gauges into the metrics registry.
    fn report_metrics(
        inner: &InMemInner,
        namespace: &[u8],
        task_id: &str,
        labels: &crate::runtime::metrics::MetricsLabels,
    ) {
        use crate::runtime::metrics::set_task_gauge;
        use metrics::{
            METRIC_WO_STATE_KEY_STATES_BYTES, METRIC_WO_STATE_KEY_STATES_COUNT,
            METRIC_WO_STATE_RAW_BYTES, METRIC_WO_STATE_RAW_COUNT, METRIC_WO_STATE_TILES_BYTES,
            METRIC_WO_STATE_TILES_COUNT, METRIC_WO_STATE_TRIGGERS_BYTES,
            METRIC_WO_STATE_TRIGGERS_COUNT,
        };

        let mut raw_count = 0u64;
        let mut raw_bytes = 0u64;
        let mut tiles_count = 0u64;
        let mut tiles_bytes = 0u64;
        let mut triggers_count = 0u64;
        let mut triggers_bytes = 0u64;
        let mut key_states_count = 0u64;
        let mut key_states_bytes = 0u64;
        for entry in inner.partitions.iter() {
            if entry.key().namespace.as_slice() != namespace {
                continue;
            }
            let part = entry.value().read();
            key_states_count += 1;
            key_states_bytes = key_states_bytes
                .saturating_add(bincode::serialized_size(&part.meta).unwrap_or(0));
            raw_count = raw_count.saturating_add(part.raw.len() as u64);
            raw_bytes = raw_bytes.saturating_add(part.raw.memory_size() as u64);
            for tiles in part.tiles.values() {
                tiles_count += 1;
                tiles_bytes =
                    tiles_bytes.saturating_add(bincode::serialized_size(tiles).unwrap_or(0));
            }
        }
        if let Some(slot) = inner.triggers.get(namespace) {
            for trigger in slot.read().iter() {
                triggers_count += 1;
                triggers_bytes =
                    triggers_bytes.saturating_add(bincode::serialized_size(trigger).unwrap_or(0));
            }
        }
        let labels = Some(labels);
        for (name, value) in [
            (METRIC_WO_STATE_RAW_COUNT, raw_count),
            (METRIC_WO_STATE_RAW_BYTES, raw_bytes),
            (METRIC_WO_STATE_TILES_COUNT, tiles_count),
            (METRIC_WO_STATE_TILES_BYTES, tiles_bytes),
            (METRIC_WO_STATE_TRIGGERS_COUNT, triggers_count),
            (METRIC_WO_STATE_TRIGGERS_BYTES, triggers_bytes),
            (METRIC_WO_STATE_KEY_STATES_COUNT, key_states_count),
            (METRIC_WO_STATE_KEY_STATES_BYTES, key_states_bytes),
        ] {
            set_task_gauge(name, value as f64, task_id, labels);
        }
    }

    /// Returns number of raw rows pruned.
    fn prune_namespace(&self, namespace: &[u8], watermark: i64, floor: i64) -> u64 {
        if let Some(slot) = self.try_triggers(namespace) {
            slot.write().retain(|trigger| trigger.fire_at.ts > watermark);
        }
        let mut pruned_rows = 0u64;
        for entry in self.inner.partitions.iter() {
            if entry.key().namespace.as_slice() != namespace {
                continue;
            }
            let mut part = entry.value().write();
            pruned_rows += part.raw.prune_before(floor);
            part.tiles.retain(|&(granularity, start_ts), _| {
                start_ts + granularity.to_millis() > floor
            });
        }
        pruned_rows
    }

    fn select_tiles(state: &PartitionState, runs: &[TileRun]) -> TileMap {
        let mut out = TileMap::new();
        for run in runs {
            let start = (run.granularity, run.start_ts);
            let end = (run.granularity, run.end_ts_exclusive);
            out.extend(
                state
                    .tiles
                    .range(start..end)
                    .map(|(key, value)| (*key, value.clone())),
            );
        }
        out
    }

    fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut bytes, batch.schema().as_ref())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(bytes)
    }

    fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
        FileReader::try_new(IoCursor::new(bytes), None)?
            .next()
            .transpose()?
            .ok_or_else(|| anyhow!("in-memory checkpoint contains an empty Arrow payload"))
    }
}

#[async_trait]
impl WindowOperatorStore for InMemWindowStore {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState> {
        let Some(slot) = self.try_partition(partition) else {
            return Ok(KeyState::default());
        };
        let meta = slot.read().meta.clone();
        Ok(meta)
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        let Some(slot) = self.try_partition(partition) else {
            return Ok(Vec::new());
        };
        let raw = slot.read().raw.select(runs);
        Ok(raw)
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        let Some(slot) = self.try_partition(partition) else {
            return Ok(TileMap::new());
        };
        let state = slot.read();
        let tiles = Self::select_tiles(&state, runs);
        Ok(tiles)
    }

    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        meta: &KeyState,
        triggers: &[WindowTrigger],
    ) -> Result<()> {
        anyhow::ensure!(
            triggers.iter().all(|trigger| &trigger.partition == partition),
            "window trigger partition does not match committed partition"
        );
        let gate = self.namespace_gate(&partition.namespace);
        let _freeze = gate.read();
        {
            let slot = self.partition_slot(partition);
            let mut state = slot.write();
            state.ts_column_index = ts_column_index;
            state.raw.insert(events, ts_column_index)?;
            for (key, value) in tiles {
                state.tiles.insert(*key, value.clone());
            }
            state.meta = meta.clone();
        }
        if !triggers.is_empty() {
            self.trigger_slot(&partition.namespace)
                .write()
                .extend(triggers.iter().cloned());
        }
        Ok(())
    }

    fn stream_due<'a>(
        &'a self,
        namespace: &'a StateNamespace,
        after: Option<Cursor>,
        through: Cursor,
    ) -> DueWorkStream<'a> {
        const PAGE_SIZE: usize = 256;

        let store = self.clone();
        let namespace = namespace.bytes.clone();
        Box::pin(futures::stream::try_unfold(
            (store, None::<WindowTrigger>),
            move |(store, resume_after)| {
                let namespace = namespace.clone();
                async move {
                    let selected = {
                        let Some(slot) = store.try_triggers(&namespace) else {
                            return Ok(None);
                        };
                        let triggers = slot.read();
                        triggers
                            .iter()
                            .filter(|trigger| after.map_or(true, |after| trigger.fire_at > after))
                            .filter(|trigger| trigger.fire_at <= through)
                            .filter(|trigger| {
                                resume_after
                                    .as_ref()
                                    .map_or(true, |resume_after| *trigger > resume_after)
                            })
                            .take(PAGE_SIZE)
                            .cloned()
                            .collect::<Vec<_>>()
                    };
                    let Some(next) = selected.last().cloned() else {
                        return Ok(None);
                    };

                    let mut grouped = BTreeMap::<PartitionKey, Vec<WindowTrigger>>::new();
                    for trigger in selected {
                        grouped
                            .entry(trigger.partition.clone())
                            .or_default()
                            .push(trigger);
                    }
                    let work = grouped
                        .into_iter()
                        .map(|(partition, triggers)| {
                            let key_state = store
                                .try_partition(&partition)
                                .map(|slot| {
                                    let meta = slot.read().meta.clone();
                                    meta
                                })
                                .unwrap_or_default();
                            DueWindowWork {
                                key_state,
                                partition,
                                triggers,
                            }
                        })
                        .collect();
                    Ok(Some((work, (store, Some(next)))))
                }
            },
        ))
    }

    async fn store_key_state(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()> {
        let gate = self.namespace_gate(&partition.namespace);
        let _freeze = gate.read();
        let slot = self.partition_slot(partition);
        slot.write().meta = meta.clone();
        Ok(())
    }

    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowBackendSnapshot> {
        let cloned = {
            let gate = self.namespace_gate(&namespace.bytes);
            let _freeze = gate.write();
            let partitions = self
                .inner
                .partitions
                .iter()
                .filter(|entry| entry.key().namespace.as_slice() == namespace.bytes.as_slice())
                .map(|entry| (entry.key().clone(), entry.value().read().clone()))
                .collect::<HashMap<_, _>>();
            let triggers = self
                .try_triggers(&namespace.bytes)
                .map(|slot| slot.read().iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            (partitions, triggers)
        };
        let (partitions, triggers) = cloned;
        let snapshot = partitions
            .into_iter()
            .map(|(partition, state)| {
                let raw = state
                    .raw
                    .batch
                    .as_ref()
                    .map(Self::encode_batch)
                    .transpose()?;
                Ok((
                    partition,
                    PartitionCheckpoint {
                        meta: state.meta,
                        raw,
                        ts_column_index: state.ts_column_index,
                        tiles: state.tiles,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        let checkpoint = InMemCheckpoint {
            namespace: namespace.bytes.clone(),
            partitions: snapshot,
            triggers,
        };
        let snapshot = bincode::serialize(&checkpoint)?;
        println!(
            "[WINDOW][INMEM] checkpoint size={} bytes (limit={} bytes, partitions={}, triggers={})",
            snapshot.len(),
            MAX_INLINE_CHECKPOINT_BYTES,
            checkpoint.partitions.len(),
            checkpoint.triggers.len(),
        );
        validate_checkpoint_size(&snapshot)?;
        Ok(WindowBackendSnapshot::InMemory { snapshot })
    }

    async fn restore(
        &self,
        namespace: &StateNamespace,
        restore: &WindowBackendSnapshot,
    ) -> Result<()> {
        let WindowBackendSnapshot::InMemory { snapshot } = restore else {
            return Err(anyhow!(
                "in-memory window store requires in-memory restore data"
            ));
        };
        validate_checkpoint_size(snapshot)?;
        let checkpoint: InMemCheckpoint = bincode::deserialize(snapshot)?;
        anyhow::ensure!(
            checkpoint.namespace.as_slice() == namespace.bytes.as_slice(),
            "in-memory checkpoint namespace does not match runtime namespace",
        );
        let restored = checkpoint
            .partitions
            .into_iter()
            .map(|(partition, state)| {
                let raw = match state.raw {
                    Some(bytes) => {
                        let batch = Self::decode_batch(&bytes)?;
                        PackedRaw::from_batch(&batch, state.ts_column_index)?
                    }
                    None => PackedRaw::default(),
                };
                Ok((
                    partition,
                    PartitionState {
                        meta: state.meta,
                        raw,
                        tiles: state.tiles,
                        ts_column_index: state.ts_column_index,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let gate = self.namespace_gate(&namespace.bytes);
        let _freeze = gate.write();
        self.inner
            .partitions
            .retain(|partition, _| partition.namespace.as_slice() != namespace.bytes.as_slice());
        for (partition, state) in restored {
            self.inner
                .partitions
                .insert(partition, Arc::new(RwLock::new(state)));
        }
        *self.trigger_slot(&namespace.bytes).write() =
            checkpoint.triggers.into_iter().collect();
        Ok(())
    }
}

#[async_trait]
impl WindowRequestStore for InMemWindowStore {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData> {
        let Some(slot) = self.try_partition(partition) else {
            return Ok(WindowData::new(Vec::new(), TileMap::new()));
        };
        let state = slot.read();
        Ok(WindowData::new(
            state.raw.select(raw_runs),
            Self::select_tiles(&state, tile_runs),
        ))
    }
}

#[async_trait]
impl OperatorStore for InMemWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics_labels(&self) -> Option<&MetricsLabels> {
        self.metrics_labels.as_ref()
    }

    async fn maintain(&self, ns: &StateNamespace, state: &dyn OperatorTaskState) -> Result<()> {
        let Some(wo) = state.as_any().downcast_ref::<WindowOperatorState>() else {
            return Ok(());
        };
        let Some((watermark, floor)) = wo.retention_cutoff() else {
            return Ok(());
        };
        let task_id = state.task_id();
        let gate = self.namespace_gate(ns.bytes.as_slice());
        let _freeze = gate.write();
        let pruned_rows = self.prune_namespace(ns.bytes.as_slice(), watermark, floor);
        if let Some(labels) = self.metrics_labels.as_ref() {
            Self::report_metrics(&self.inner, ns.bytes.as_slice(), task_id, labels);
            drop(_freeze);
            metrics::add_pruned(task_id, labels, pruned_rows);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    use crate::test_utils::window_aggs as test_utils;
    use crate::runtime::operators::window::model::{
        KeyEvaluationState, TileRun, TimeGranularity, WindowTiles, WindowTriggerKind,
    };
    use crate::runtime::operators::window::state::WindowOperatorState;
    use crate::runtime::operators::window::store::{data::RowIdx, StateVersion};
    use crate::runtime::state::OperatorStore;

    fn partition() -> PartitionKey {
        PartitionKey {
            namespace: b"test-namespace".to_vec(),
            business_key: b"test-key".to_vec(),
        }
    }

    fn batch(cursors: &[(i64, u64)]) -> RecordBatch {
        let rows = cursors
            .iter()
            .map(|&(ts, seq_no)| (ts, ts as f64, "key", seq_no))
            .collect::<Vec<_>>();
        test_utils::batch(&rows)
    }

    fn raw_run(from: (i64, u64), to: (i64, u64)) -> RawRun {
        RawRun {
            from: Cursor::new(from.0, from.1),
            to: Cursor::new(to.0, to.1),
        }
    }

    fn raw_cursors(batches: &[RecordBatch]) -> Vec<Cursor> {
        batches
            .iter()
            .flat_map(|batch| cursors_from_batch(batch, 0).unwrap())
            .collect()
    }

    fn tiles(entries: &[(TimeGranularity, i64, usize)]) -> TileMap {
        entries
            .iter()
            .map(|&(granularity, start_ts, window_id)| {
                let mut value = WindowTiles::default();
                value.windows.insert(window_id, Default::default());
                ((granularity, start_ts), value)
            })
            .collect()
    }

    fn tile_keys(tiles: &TileMap) -> Vec<(TimeGranularity, i64)> {
        tiles.keys().copied().collect()
    }

    fn assert_meta(actual: &KeyState, expected: &KeyState) {
        assert_eq!(actual, expected);
    }

    fn assert_packed(batches: &[RecordBatch], rows: usize) {
        assert_eq!(batches.len(), 1, "expected one packed batch, got {}", batches.len());
        assert_eq!(batches[0].num_rows(), rows);
    }

    #[tokio::test]
    async fn missing_partition_and_empty_runs_return_defaults() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let raw_runs = [raw_run((0, 0), (10, 0))];
        let tile_runs = [TileRun {
            granularity: TimeGranularity::Seconds(1),
            start_ts: 0,
            end_ts_exclusive: 10_000,
        }];

        assert_meta(
            &store.load_key_state(&partition).await.unwrap(),
            &KeyState::default(),
        );
        assert!(store
            .load_raw(&partition, &raw_runs)
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .load_tiles(&partition, &tile_runs)
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .load_window_data(&partition, &raw_runs, &tile_runs)
            .await
            .unwrap()
            .raw_batches()
            .is_empty());

        store
            .store_key_state(
                &partition,
                &KeyState {
                    next_seq: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(store.load_raw(&partition, &[]).await.unwrap().is_empty());
        assert!(store.load_tiles(&partition, &[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn raw_ranges_are_half_open_globally_ordered_and_deduplicated() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let events = batch(&[(30, 3), (10, 1), (20, 2), (20, 1), (40, 4)]);
        store
            .commit_events(
                &partition,
                0,
                &events,
                &TileMap::new(),
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();

        let loaded = store
            .load_raw(
                &partition,
                &[raw_run((10, 1), (30, 3)), raw_run((20, 1), (40, 4))],
            )
            .await
            .unwrap();

        assert_packed(&loaded, 4);
        assert_eq!(
            raw_cursors(&loaded),
            vec![
                Cursor::new(10, 1),
                Cursor::new(20, 1),
                Cursor::new(20, 2),
                Cursor::new(30, 3),
            ]
        );
    }

    #[tokio::test]
    async fn commit_events_packs_rows_into_one_batch_per_key() {
        let store = InMemWindowStore::new();
        let partition = partition();
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(30, 3), (10, 1), (20, 2)]),
                &TileMap::new(),
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(40, 4), (50, 5)]),
                &TileMap::new(),
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();

        let loaded = store
            .load_raw(&partition, &[raw_run((0, 0), (100, 0))])
            .await
            .unwrap();
        assert_packed(&loaded, 5);
        assert_eq!(
            raw_cursors(&loaded),
            vec![
                Cursor::new(10, 1),
                Cursor::new(20, 2),
                Cursor::new(30, 3),
                Cursor::new(40, 4),
                Cursor::new(50, 5),
            ]
        );
    }

    #[tokio::test]
    async fn tile_ranges_are_exact_and_half_open() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let stored_tiles = tiles(&[
            (TimeGranularity::Seconds(1), 0, 0),
            (TimeGranularity::Seconds(1), 1_000, 1),
            (TimeGranularity::Seconds(1), 2_000, 2),
            (TimeGranularity::Seconds(1), 3_000, 3),
            (TimeGranularity::Minutes(1), 1_000, 4),
        ]);
        store
            .commit_events(
                &partition,
                0,
                &batch(&[]),
                &stored_tiles,
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();

        let loaded = store
            .load_tiles(
                &partition,
                &[TileRun {
                    granularity: TimeGranularity::Seconds(1),
                    start_ts: 1_000,
                    end_ts_exclusive: 3_000,
                }],
            )
            .await
            .unwrap();

        assert_eq!(
            tile_keys(&loaded),
            vec![
                (TimeGranularity::Seconds(1), 1_000),
                (TimeGranularity::Seconds(1), 2_000),
            ]
        );
    }

    #[tokio::test]
    async fn store_key_state_leaves_raw_and_tiles_unchanged() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let stored_tiles = tiles(&[(TimeGranularity::Seconds(1), 1_000, 7)]);
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(1_000, 4)]),
                &stored_tiles,
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();
        let updated_meta = KeyState {
            next_seq: 6,
            evaluation: Some(KeyEvaluationState {
                through: Cursor::new(1_000, 4),
                accumulators: BTreeMap::new(),
            }),
        };

        store
            .store_key_state(&partition, &updated_meta)
            .await
            .unwrap();

        assert_meta(
            &store.load_key_state(&partition).await.unwrap(),
            &updated_meta,
        );
        assert_eq!(
            raw_cursors(
                &store
                    .load_raw(&partition, &[raw_run((0, 0), (2_000, 0))])
                    .await
                    .unwrap()
            ),
            vec![Cursor::new(1_000, 4)]
        );
        assert_eq!(
            tile_keys(
                &store
                    .load_tiles(
                        &partition,
                        &[TileRun {
                            granularity: TimeGranularity::Seconds(1),
                            start_ts: 0,
                            end_ts_exclusive: 2_000,
                        }]
                    )
                    .await
                    .unwrap()
            ),
            vec![(TimeGranularity::Seconds(1), 1_000)]
        );
    }

    #[tokio::test]
    async fn commit_events_stores_raw_tiles_and_meta_coherently() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let meta = KeyState {
            next_seq: 3,
            ..Default::default()
        };
        let stored_tiles = tiles(&[(TimeGranularity::Seconds(1), 2_000, 3)]);

        store
            .commit_events(
                &partition,
                0,
                &batch(&[(2_000, 2), (1_000, 1)]),
                &stored_tiles,
                &meta,
                &[],
            )
            .await
            .unwrap();

        assert_meta(&store.load_key_state(&partition).await.unwrap(), &meta);
        let loaded = store
            .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
            .await
            .unwrap();
        assert_packed(&loaded, 2);
        assert_eq!(
            raw_cursors(&loaded),
            vec![Cursor::new(1_000, 1), Cursor::new(2_000, 2)]
        );
        assert_eq!(
            tile_keys(
                &store
                    .load_tiles(
                        &partition,
                        &[TileRun {
                            granularity: TimeGranularity::Seconds(1),
                            start_ts: 2_000,
                            end_ts_exclusive: 3_000,
                        }]
                    )
                    .await
                    .unwrap()
            ),
            vec![(TimeGranularity::Seconds(1), 2_000)]
        );
    }

    #[tokio::test]
    async fn load_window_data_returns_combined_raw_and_tile_view() {
        let store = InMemWindowStore::new();
        let partition = partition();
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(1_000, 1), (2_000, 2), (3_000, 3)]),
                &tiles(&[
                    (TimeGranularity::Seconds(1), 1_000, 7),
                    (TimeGranularity::Seconds(1), 2_000, 7),
                    (TimeGranularity::Seconds(1), 3_000, 7),
                ]),
                &KeyState::default(),
                &[],
            )
            .await
            .unwrap();

        let data = store
            .load_window_data(
                &partition,
                &[raw_run((2_000, 2), (4_000, 0))],
                &[TileRun {
                    granularity: TimeGranularity::Seconds(1),
                    start_ts: 1_000,
                    end_ts_exclusive: 3_000,
                }],
            )
            .await
            .unwrap();
        assert_packed(data.raw_batches(), 2);
        assert_eq!(
            raw_cursors(data.raw_batches()),
            vec![Cursor::new(2_000, 2), Cursor::new(3_000, 3)]
        );

        let window_expr = test_utils::window_expr_from_sql(
            "SELECT sum(value) OVER (
                PARTITION BY partition_key ORDER BY timestamp
                RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
            ) FROM test_table",
        )
        .await;
        let view = data.for_window(7, 0, &window_expr);
        assert_eq!(view.nav.cursor(RowIdx(0)), Cursor::new(2_000, 2));
        assert_eq!(
            view.nav.cursor(view.nav.next(RowIdx(0)).unwrap()),
            Cursor::new(3_000, 3)
        );
        assert_eq!(
            view.tiles
                .iter()
                .map(|tile| (tile.granularity, tile.tile_start))
                .collect::<Vec<_>>(),
            vec![
                (TimeGranularity::Seconds(1), 1_000),
                (TimeGranularity::Seconds(1), 2_000),
            ]
        );
    }

    #[tokio::test]
    async fn due_triggers_are_range_filtered_and_backend_paged() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let namespace = StateNamespace::new(&partition.namespace);
        let triggers = (0..300)
            .map(|seq_no| WindowTrigger {
                fire_at: Cursor::new(seq_no as i64, seq_no),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            })
            .collect::<Vec<_>>();
        store
            .commit_events(
                &partition,
                0,
                &batch(&[]),
                &TileMap::new(),
                &KeyState::default(),
                &triggers,
            )
            .await
            .unwrap();

        let mut due = store.stream_due(
            &namespace,
            Some(Cursor::new(9, u64::MAX)),
            Cursor::new(299, u64::MAX),
        );
        let first = due.try_next().await.unwrap().unwrap();
        assert_eq!(first[0].triggers.len(), 256);
        let second = due.try_next().await.unwrap().unwrap();
        assert_eq!(second[0].triggers.len(), 34);
        assert!(due.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn maintain_prunes_triggers_raw_and_tiles() {
        let store = InMemWindowStore::new();
        let partition = partition();
        let namespace = StateNamespace::new(&partition.namespace);
        let triggers = [
            WindowTrigger {
                fire_at: Cursor::new(1_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            },
            WindowTrigger {
                fire_at: Cursor::new(5_000, 2),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            },
            WindowTrigger {
                fire_at: Cursor::new(10_000, 3),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            },
        ];
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(1_000, 1), (5_000, 2), (10_000, 3)]),
                &tiles(&[
                    (TimeGranularity::Seconds(1), 1_000, 7),
                    (TimeGranularity::Seconds(1), 5_000, 7),
                    (TimeGranularity::Seconds(1), 10_000, 7),
                ]),
                &KeyState {
                    next_seq: 4,
                    ..Default::default()
                },
                &triggers,
            )
            .await
            .unwrap();

        let task_state = WindowOperatorState::new(
            Arc::new(store.clone()) as Arc<dyn WindowOperatorStore>,
            namespace.clone(),
            Arc::from("test-task"),
            0,
            Arc::new(BTreeMap::new()),
            0,
            0,
        );
        task_state
            .watermark_frontier
            .store(5_000, std::sync::atomic::Ordering::Release);
        store.maintain(&namespace, &task_state).await.unwrap();

        let mut due = store.stream_due(&namespace, None, Cursor::new(10_000, u64::MAX));
        let work = due.try_next().await.unwrap().unwrap();
        assert_eq!(work[0].triggers, vec![triggers[2].clone()]);
        assert!(due.try_next().await.unwrap().is_none());

        let loaded = store
            .load_raw(&partition, &[raw_run((0, 0), (20_000, 0))])
            .await
            .unwrap();
        assert_packed(&loaded, 2);
        assert_eq!(
            raw_cursors(&loaded),
            vec![Cursor::new(5_000, 2), Cursor::new(10_000, 3)]
        );
        assert_eq!(
            tile_keys(
                &store
                    .load_tiles(
                        &partition,
                        &[TileRun {
                            granularity: TimeGranularity::Seconds(1),
                            start_ts: 0,
                            end_ts_exclusive: 20_000,
                        }]
                    )
                    .await
                    .unwrap()
            ),
            vec![
                (TimeGranularity::Seconds(1), 5_000),
                (TimeGranularity::Seconds(1), 10_000)
            ]
        );
        assert_eq!(store.load_key_state(&partition).await.unwrap().next_seq, 4);
    }

    #[tokio::test]
    async fn checkpoint_restores_complete_namespace_into_fresh_store() {
        let source = InMemWindowStore::new();
        let partition = partition();
        let namespace = StateNamespace::new(&partition.namespace);
        let mut accumulators = BTreeMap::new();
        accumulators.insert(
            7,
            vec![datafusion::scalar::ScalarValue::Float64(Some(3.0))],
        );
        let meta = KeyState {
            next_seq: 3,
            evaluation: Some(KeyEvaluationState {
                through: Cursor::new(1_000, 1),
                accumulators,
            }),
        };
        let triggers = [
            WindowTrigger {
                fire_at: Cursor::new(1_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            },
            WindowTrigger {
                fire_at: Cursor::new(2_000, 2),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            },
        ];
        source
            .commit_events(
                &partition,
                0,
                &batch(&[(1_000, 1), (2_000, 2)]),
                &tiles(&[(TimeGranularity::Seconds(1), 1_000, 7)]),
                &meta,
                &triggers,
            )
            .await
            .unwrap();

        let checkpoint = source.checkpoint(&namespace).await.unwrap();
        let restored = InMemWindowStore::new();
        restored.restore(&namespace, &checkpoint).await.unwrap();

        assert_meta(&restored.load_key_state(&partition).await.unwrap(), &meta);
        let mut due = restored.stream_due(
            &namespace,
            Some(Cursor::new(1_000, u64::MAX)),
            Cursor::new(2_000, u64::MAX),
        );
        let work = due.try_next().await.unwrap().unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].triggers, vec![triggers[1].clone()]);
        let loaded = restored
            .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
            .await
            .unwrap();
        assert_packed(&loaded, 2);
        assert_eq!(
            raw_cursors(&loaded),
            vec![Cursor::new(1_000, 1), Cursor::new(2_000, 2)]
        );
        assert_eq!(
            tile_keys(
                &restored
                    .load_tiles(
                        &partition,
                        &[TileRun {
                            granularity: TimeGranularity::Seconds(1),
                            start_ts: 0,
                            end_ts_exclusive: 2_000,
                        }]
                    )
                    .await
                    .unwrap()
            ),
            vec![(TimeGranularity::Seconds(1), 1_000)]
        );
    }

    #[tokio::test]
    async fn restore_replaces_only_the_checkpoint_namespace() {
        let source = InMemWindowStore::new();
        let checkpoint_partition = partition();
        let checkpoint_namespace = StateNamespace::new(&checkpoint_partition.namespace);
        source
            .store_key_state(
                &checkpoint_partition,
                &KeyState {
                    next_seq: 11,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let checkpoint = source.checkpoint(&checkpoint_namespace).await.unwrap();

        let restored = InMemWindowStore::new();
        let other_partition = PartitionKey {
            namespace: b"other-namespace".to_vec(),
            business_key: b"other-key".to_vec(),
        };
        restored
            .store_key_state(
                &other_partition,
                &KeyState {
                    next_seq: 22,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        restored
            .restore(&checkpoint_namespace, &checkpoint)
            .await
            .unwrap();

        assert_eq!(
            restored
                .load_key_state(&checkpoint_partition)
                .await
                .unwrap()
                .next_seq,
            11
        );
        assert_eq!(
            restored
                .load_key_state(&other_partition)
                .await
                .unwrap()
                .next_seq,
            22
        );
    }

    #[tokio::test]
    async fn concurrent_partition_writes_do_not_share_a_process_wide_lock() {
        let store = InMemWindowStore::new();
        let futs = (0..64u64).map(|i| {
            let store = store.clone();
            async move {
                let partition = PartitionKey {
                    namespace: b"test-namespace".to_vec(),
                    business_key: format!("key-{i}").into_bytes(),
                };
                store
                    .commit_events(
                        &partition,
                        0,
                        &batch(&[(i as i64, i)]),
                        &TileMap::new(),
                        &KeyState {
                            next_seq: i + 1,
                            ..Default::default()
                        },
                        &[],
                    )
                    .await
                    .unwrap();
                store
                    .store_key_state(
                        &partition,
                        &KeyState {
                            next_seq: i + 10,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                store.load_key_state(&partition).await.unwrap().next_seq
            }
        });
        let got = futures::future::join_all(futs).await;
        let mut got = got;
        got.sort_unstable();
        let expected: Vec<u64> = (10..74).collect();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn in_memory_restore_rejects_version_checkpoint() {
        let store = InMemWindowStore::new();
        let error = store
            .restore(
                &StateNamespace::new("test-namespace"),
                &WindowBackendSnapshot::Versioned {
                    version: StateVersion {
                        attempt: b"attempt".to_vec(),
                        epoch: 1,
                    },
                },
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("requires in-memory restore data"));
    }
}
