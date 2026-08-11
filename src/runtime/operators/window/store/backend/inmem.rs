//! In-memory window store implementation and behavior tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor as IoCursor;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use std::any::Any;

use crate::runtime::operators::window::metrics::WindowOperatorMetrics;
use crate::runtime::operators::window::model::{
    Cursor, RawRun, TileRun, WindowTiles, WindowTrigger,
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

#[derive(Debug, Clone, Default)]
struct PartitionState {
    meta: KeyState,
    raw: BTreeMap<Cursor, RecordBatch>,
    tiles: TileMap,
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
    raw: BTreeMap<Cursor, Vec<u8>>,
    tiles: TileMap,
}

#[derive(Debug, Default)]
struct InMemState {
    partitions: HashMap<PartitionKey, PartitionState>,
    triggers: BTreeSet<WindowTrigger>,
}

/// Development/test reference backend with inline checkpoints limited to 8 MiB.
/// One read lock is the WRO snapshot boundary. Physical prune runs via
/// [`OperatorStore::maintain`], not the WO watermark hot path.
#[derive(Debug, Clone, Default)]
pub struct InMemWindowStore {
    state: Arc<RwLock<InMemState>>,
}

impl InMemWindowStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compute logical namespace metrics on demand (no cached size counters).
    pub async fn sample_namespace_metrics(
        &self,
        namespace: &StateNamespace,
    ) -> WindowOperatorMetrics {
        let state = self.state.read().await;
        Self::namespace_metrics(&state, namespace.bytes.as_slice())
    }

    fn namespace_metrics(state: &InMemState, namespace: &[u8]) -> WindowOperatorMetrics {
        let mut snap = WindowOperatorMetrics::default();
        for (partition, part) in &state.partitions {
            if partition.namespace.as_slice() != namespace {
                continue;
            }
            snap.key_states_count += 1;
            snap.key_states_bytes = snap
                .key_states_bytes
                .saturating_add(bincode::serialized_size(&part.meta).unwrap_or(0));
            for batch in part.raw.values() {
                snap.raw_count += 1;
                snap.raw_bytes = snap
                    .raw_bytes
                    .saturating_add(batch.get_array_memory_size() as u64);
            }
            for tiles in part.tiles.values() {
                snap.tiles_count += 1;
                snap.tiles_bytes = snap
                    .tiles_bytes
                    .saturating_add(bincode::serialized_size(tiles).unwrap_or(0));
            }
        }
        for trigger in &state.triggers {
            if trigger.partition.namespace.as_slice() != namespace {
                continue;
            }
            snap.triggers_count += 1;
            snap.triggers_bytes = snap
                .triggers_bytes
                .saturating_add(bincode::serialized_size(trigger).unwrap_or(0));
        }
        snap
    }

    fn prune_namespace(state: &mut InMemState, namespace: &[u8], watermark: i64, floor: i64) {
        state.triggers.retain(|trigger| {
            trigger.partition.namespace.as_slice() != namespace || trigger.fire_at.ts > watermark
        });
        for (partition, part) in state.partitions.iter_mut() {
            if partition.namespace.as_slice() != namespace {
                continue;
            }
            part.raw.retain(|cursor, _| cursor.ts >= floor);
            part.tiles.retain(|&(granularity, start_ts), _| {
                start_ts + granularity.to_millis() > floor
            });
        }
    }

    fn select_raw(state: &PartitionState, runs: &[RawRun]) -> Vec<RecordBatch> {
        let mut out = BTreeMap::new();
        for run in runs {
            for (&cursor, batch) in state.raw.range(run.from..run.to) {
                out.insert(cursor, batch.clone());
            }
        }
        out.into_values().collect()
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
        let state = self.state.read().await;
        Ok(state
            .partitions
            .get(partition)
            .map(|state| state.meta.clone())
            .unwrap_or_default())
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        let state = self.state.read().await;
        Ok(state
            .partitions
            .get(partition)
            .map(|state| Self::select_raw(state, runs))
            .unwrap_or_default())
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        let state = self.state.read().await;
        Ok(state
            .partitions
            .get(partition)
            .map(|state| Self::select_tiles(state, runs))
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
        let cursors = cursors_from_batch(events, ts_column_index)?;
        anyhow::ensure!(
            triggers.iter().all(|trigger| &trigger.partition == partition),
            "window trigger partition does not match committed partition"
        );
        let mut store = self.state.write().await;
        let state = store.partitions.entry(partition.clone()).or_default();
        for (row, cursor) in cursors.into_iter().enumerate() {
            state.raw.insert(cursor, events.slice(row, 1));
        }
        for (key, value) in tiles {
            state.tiles.insert(*key, value.clone());
        }
        state.meta = meta.clone();
        store.triggers.extend(triggers.iter().cloned());
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
                    let state = store.state.read().await;
                    let selected = state
                        .triggers
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
                            key_state: state
                                .partitions
                                .get(&partition)
                                .map(|partition_state| partition_state.meta.clone())
                                .unwrap_or_default(),
                            partition,
                            triggers,
                        })
                        .collect();
                    drop(state);
                    Ok(Some((work, (store, Some(next)))))
                }
            },
        ))
    }

    async fn store_key_state(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()> {
        let mut store = self.state.write().await;
        let state = store.partitions.entry(partition.clone()).or_default();
        state.meta = meta.clone();
        Ok(())
    }

    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowBackendSnapshot> {
        let store = self.state.read().await;
        let snapshot = store
            .partitions
            .iter()
            .filter(|(partition, _)| partition.namespace.as_slice() == namespace.bytes.as_slice())
            .map(|(partition, state)| {
                let raw = state
                    .raw
                    .iter()
                    .map(|(cursor, batch)| Ok((*cursor, Self::encode_batch(batch)?)))
                    .collect::<Result<_>>()?;
                Ok((
                    partition.clone(),
                    PartitionCheckpoint {
                        meta: state.meta.clone(),
                        raw,
                        tiles: state.tiles.clone(),
                    },
                ))
            })
            .collect::<Result<_>>()?;
        let checkpoint = InMemCheckpoint {
            namespace: namespace.bytes.clone(),
            partitions: snapshot,
            triggers: store
                .triggers
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
                let raw = state
                    .raw
                    .into_iter()
                    .map(|(cursor, bytes)| Ok((cursor, Self::decode_batch(&bytes)?)))
                    .collect::<Result<_>>()?;
                Ok((
                    partition,
                    PartitionState {
                        meta: state.meta,
                        raw,
                        tiles: state.tiles,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut store = self.state.write().await;
        store
            .partitions
            .retain(|partition, _| partition.namespace.as_slice() != namespace.bytes.as_slice());
        store.partitions.extend(restored);
        store
            .triggers
            .retain(|trigger| trigger.partition.namespace.as_slice() != namespace.bytes.as_slice());
        store.triggers.extend(checkpoint.triggers);
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
        let store = self.state.read().await;
        let Some(state) = store.partitions.get(partition) else {
            return Ok(WindowData::new(Vec::new(), TileMap::new()));
        };
        Ok(WindowData::new(
            Self::select_raw(state, raw_runs),
            Self::select_tiles(state, tile_runs),
        ))
    }
}

#[async_trait]
impl OperatorStore for InMemWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn maintain(
        &self,
        ns: &StateNamespace,
        state: &dyn OperatorTaskState,
    ) -> Result<()> {
        let Some(wo) = state.as_any().downcast_ref::<WindowOperatorState>() else {
            return Ok(());
        };
        let Some((watermark, floor)) = wo.retention_cutoff() else {
            return Ok(());
        };
        let mut store = self.state.write().await;
        Self::prune_namespace(&mut store, ns.bytes.as_slice(), watermark, floor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    use crate::runtime::operators::window::aggs::test_utils;
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
            0,
            Arc::new(BTreeMap::new()),
            0,
            0,
        );
        task_state
            .watermark_frontier
            .store(5_000, std::sync::atomic::Ordering::Release);
        store
            .maintain(&namespace, &task_state)
            .await
            .unwrap();

        let mut due = store.stream_due(&namespace, None, Cursor::new(10_000, u64::MAX));
        let work = due.try_next().await.unwrap().unwrap();
        assert_eq!(work[0].triggers, vec![triggers[2].clone()]);
        assert!(due.try_next().await.unwrap().is_none());

        assert_eq!(
            raw_cursors(
                &store
                    .load_raw(&partition, &[raw_run((0, 0), (20_000, 0))])
                    .await
                    .unwrap()
            ),
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

        let size = store.sample_namespace_metrics(&namespace).await;
        assert_eq!(size.raw_count, 2);
        assert_eq!(size.tiles_count, 2);
        assert_eq!(size.triggers_count, 1);
        assert_eq!(size.key_states_count, 1);
        assert!(size.raw_bytes > 0);
        assert!(size.tiles_bytes > 0);
        assert!(size.triggers_bytes > 0);
        assert!(size.key_states_bytes > 0);
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
        assert_eq!(
            raw_cursors(
                &restored
                    .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
                    .await
                    .unwrap()
            ),
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
