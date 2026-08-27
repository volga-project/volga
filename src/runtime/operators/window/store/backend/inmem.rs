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
use crate::runtime::operators::window::model::{Cursor, RawRun, TileRun, WindowTrigger};
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

/// One cursor-ordered Arrow batch per key.
#[derive(Debug, Clone, Default)]
struct PackedRaw {
    batch: Option<RecordBatch>,
    index: BTreeMap<Cursor, u32>,
}

impl PackedRaw {
    fn rebuild(batch: &RecordBatch, ts_column_index: usize) -> Result<Self> {
        if batch.num_rows() == 0 {
            return Ok(Self::default());
        }
        let mut by_cursor = BTreeMap::new();
        for (i, cursor) in cursors_from_batch(batch, ts_column_index)?
            .into_iter()
            .enumerate()
        {
            by_cursor.insert(cursor, i as u32);
        }
        let indices: Vec<u32> = by_cursor.values().copied().collect();
        let batch = arrow::compute::take_record_batch(batch, &UInt32Array::from(indices))?;
        Ok(Self {
            batch: Some(batch),
            index: by_cursor
                .into_keys()
                .enumerate()
                .map(|(i, cursor)| (cursor, i as u32))
                .collect(),
        })
    }

    fn insert(&mut self, events: &RecordBatch, ts_column_index: usize) -> Result<()> {
        if events.num_rows() == 0 {
            return Ok(());
        }
        *self = match &self.batch {
            Some(existing) => Self::rebuild(
                &concat_batches(&existing.schema(), [existing, events])?,
                ts_column_index,
            )?,
            None => Self::rebuild(events, ts_column_index)?,
        };
        Ok(())
    }

    fn select(&self, runs: &[RawRun]) -> Vec<RecordBatch> {
        let Some(batch) = &self.batch else {
            return Vec::new();
        };
        let mut rows: Vec<u32> = runs
            .iter()
            .flat_map(|run| self.index.range(run.from..run.to).map(|(_, row)| *row))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        if rows.is_empty() {
            return Vec::new();
        }
        if rows.windows(2).all(|pair| pair[1] == pair[0] + 1) {
            return vec![batch.slice(rows[0] as usize, rows.len())];
        }
        vec![
            arrow::compute::take_record_batch(batch, &UInt32Array::from(rows))
                .expect("take raw rows"),
        ]
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
        self.batch = Some(
            arrow::compute::take_record_batch(
                self.batch.as_ref().unwrap(),
                &UInt32Array::from(indices),
            )
            .expect("take remaining raw rows"),
        );
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
    raw: Option<Vec<u8>>,
    ts_column_index: usize,
    tiles: TileMap,
}

/// Development/test reference backend with inline checkpoints limited to 8 MiB.
///
/// One packed Arrow batch per key. Each partition is an `Arc<RwLock<_>>` in a
/// DashMap so shard guards are dropped before Arrow concat/take. WRO
/// `load_window_data` is one partition read (raw + tiles). Checkpoint clones
/// partitions then triggers with no freeze and can interleave with
/// [`OperatorStore::maintain`]; that is accepted for this process-local backend.
#[derive(Debug, Clone, Default)]
pub struct InMemWindowStore {
    partitions: Arc<DashMap<PartitionKey, Arc<RwLock<PartitionState>>>>,
    triggers: Arc<RwLock<BTreeSet<WindowTrigger>>>,
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

    fn read_part<R>(
        &self,
        partition: &PartitionKey,
        f: impl FnOnce(&PartitionState) -> R,
    ) -> Option<R> {
        let slot = self.partitions.get(partition).map(|entry| entry.clone())?;
        f(&slot.read())
    }

    fn write_part<R>(
        &self,
        partition: &PartitionKey,
        f: impl FnOnce(&mut PartitionState) -> R,
    ) -> R {
        let slot = self
            .partitions
            .entry(partition.clone())
            .or_insert_with(|| Arc::new(RwLock::new(PartitionState::default())))
            .clone();
        let mut state = slot.write();
        f(&mut state)
    }

    fn report_metrics(
        &self,
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
        let mut key_states_count = 0u64;
        let mut key_states_bytes = 0u64;
        let slots: Vec<_> = self
            .partitions
            .iter()
            .filter(|entry| entry.key().namespace.as_slice() == namespace)
            .map(|entry| entry.value().clone())
            .collect();
        for slot in slots {
            let part = slot.read();
            key_states_count += 1;
            key_states_bytes =
                key_states_bytes.saturating_add(bincode::serialized_size(&part.meta).unwrap_or(0));
            raw_count = raw_count.saturating_add(part.raw.index.len() as u64);
            raw_bytes = raw_bytes.saturating_add(
                part.raw
                    .batch
                    .as_ref()
                    .map(|batch| batch.get_array_memory_size() as u64)
                    .unwrap_or(0),
            );
            for tiles in part.tiles.values() {
                tiles_count += 1;
                tiles_bytes =
                    tiles_bytes.saturating_add(bincode::serialized_size(tiles).unwrap_or(0));
            }
        }
        let mut triggers_count = 0u64;
        let mut triggers_bytes = 0u64;
        for trigger in self.triggers.read().iter() {
            if trigger.partition.namespace.as_slice() != namespace {
                continue;
            }
            triggers_count += 1;
            triggers_bytes =
                triggers_bytes.saturating_add(bincode::serialized_size(trigger).unwrap_or(0));
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

    fn select_tiles(state: &PartitionState, runs: &[TileRun]) -> TileMap {
        let mut out = TileMap::new();
        for run in runs {
            out.extend(
                state
                    .tiles
                    .range((run.granularity, run.start_ts)..(run.granularity, run.end_ts_exclusive))
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
        Ok(self
            .read_part(partition, |state| state.meta.clone())
            .unwrap_or_default())
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        Ok(self
            .read_part(partition, |state| state.raw.select(runs))
            .unwrap_or_default())
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        Ok(self
            .read_part(partition, |state| Self::select_tiles(state, runs))
            .unwrap_or_default())
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
            triggers
                .iter()
                .all(|trigger| &trigger.partition == partition),
            "window trigger partition does not match committed partition"
        );
        self.write_part(partition, |state| -> Result<()> {
            state.ts_column_index = ts_column_index;
            state.raw.insert(events, ts_column_index)?;
            for (key, value) in tiles {
                state.tiles.insert(*key, value.clone());
            }
            state.meta = meta.clone();
            Ok(())
        })?;
        if !triggers.is_empty() {
            self.triggers.write().extend(triggers.iter().cloned());
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
                    let selected = store
                        .triggers
                        .read()
                        .iter()
                        .filter(|trigger| trigger.partition.namespace == namespace)
                        .filter(|trigger| after.map_or(true, |after| trigger.fire_at > after))
                        .filter(|trigger| trigger.fire_at <= through)
                        .filter(|trigger| {
                            resume_after
                                .as_ref()
                                .map_or(true, |resume_after| *trigger > resume_after)
                        })
                        .take(PAGE_SIZE)
                        .cloned()
                        .collect::<Vec<_>>();
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
                        .map(|(partition, triggers)| DueWindowWork {
                            key_state: store
                                .read_part(&partition, |state| state.meta.clone())
                                .unwrap_or_default(),
                            partition,
                            triggers,
                        })
                        .collect();
                    Ok(Some((work, (store, Some(next)))))
                }
            },
        ))
    }

    async fn store_key_state(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()> {
        self.write_part(partition, |state| state.meta = meta.clone());
        Ok(())
    }

    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowBackendSnapshot> {
        let slots: Vec<_> = self
            .partitions
            .iter()
            .filter(|entry| entry.key().namespace.as_slice() == namespace.bytes.as_slice())
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let partitions = slots
            .into_iter()
            .map(|(partition, slot)| (partition, slot.read().clone()))
            .collect::<HashMap<_, _>>();
        let snapshot = partitions
            .into_iter()
            .map(|(partition, state)| {
                Ok((
                    partition,
                    PartitionCheckpoint {
                        meta: state.meta,
                        raw: state
                            .raw
                            .batch
                            .as_ref()
                            .map(Self::encode_batch)
                            .transpose()?,
                        ts_column_index: state.ts_column_index,
                        tiles: state.tiles,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        let checkpoint = InMemCheckpoint {
            namespace: namespace.bytes.clone(),
            partitions: snapshot,
            triggers: self
                .triggers
                .read()
                .iter()
                .filter(|trigger| trigger.partition.namespace == namespace.bytes)
                .cloned()
                .collect(),
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
                        PackedRaw::rebuild(&Self::decode_batch(&bytes)?, state.ts_column_index)?
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
            .collect::<Result<Vec<_>>>()?;
        self.partitions
            .retain(|partition, _| partition.namespace.as_slice() != namespace.bytes.as_slice());
        for (partition, state) in restored {
            self.partitions
                .insert(partition, Arc::new(RwLock::new(state)));
        }
        let mut triggers = self.triggers.write();
        triggers
            .retain(|trigger| trigger.partition.namespace.as_slice() != namespace.bytes.as_slice());
        triggers.extend(checkpoint.triggers);
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
        Ok(self
            .read_part(partition, |state| {
                WindowData::new(
                    state.raw.select(raw_runs),
                    Self::select_tiles(state, tile_runs),
                )
            })
            .unwrap_or_else(|| WindowData::new(Vec::new(), TileMap::new())))
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
        self.triggers.write().retain(|trigger| {
            trigger.partition.namespace.as_slice() != ns.bytes.as_slice()
                || trigger.fire_at.ts > watermark
        });
        let slots: Vec<_> = self
            .partitions
            .iter()
            .filter(|entry| entry.key().namespace.as_slice() == ns.bytes.as_slice())
            .map(|entry| entry.value().clone())
            .collect();
        let mut pruned_rows = 0u64;
        for slot in slots {
            let mut part = slot.write();
            pruned_rows += part.raw.prune_before(floor);
            part.tiles
                .retain(|&(granularity, start_ts), _| start_ts + granularity.to_millis() > floor);
        }
        if let Some(labels) = self.metrics_labels.as_ref() {
            self.report_metrics(ns.bytes.as_slice(), state.task_id(), labels);
            metrics::add_pruned(state.task_id(), labels, pruned_rows);
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
        store
            .commit_events(
                &partition,
                0,
                &batch(&[(50, 5)]),
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
        assert_eq!(
            raw_cursors(
                &store
                    .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
                    .await
                    .unwrap()
            ),
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
