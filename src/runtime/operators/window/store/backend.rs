use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{RawRun, TileRun};

use super::key_state::KeyState;
use super::keys::{PartitionKey, StateNamespace};
use super::row_nav::cursors_from_batch;
use super::window_data::{TileMap, WindowData};

pub type AttemptToken = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowCheckpointMeta {
    pub version: StateVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowRestoreMeta {
    pub base_version: StateVersion,
}

impl From<WindowCheckpointMeta> for WindowRestoreMeta {
    fn from(meta: WindowCheckpointMeta) -> Self {
        Self {
            base_version: meta.version,
        }
    }
}

/// Store operations used by the sole Window Operator for a partition.
#[async_trait]
pub trait WindowOperatorStore: Send + Sync + std::fmt::Debug {
    async fn load_meta(&self, partition: &PartitionKey) -> Result<KeyState>;
    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>>;
    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap>;
    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        meta: &KeyState,
    ) -> Result<()>;
    async fn store_meta(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()>;
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowCheckpointMeta>;
    async fn restore(
        &self,
        namespace: &StateNamespace,
        meta: &WindowRestoreMeta,
    ) -> Result<()>;
}

/// Coherent point-lookup reads used by the Window Request Operator.
#[async_trait]
pub trait WindowRequestStore: Send + Sync + std::fmt::Debug {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData>;
}

#[derive(Debug, Clone, Default)]
struct PartitionState {
    meta: KeyState,
    raw: BTreeMap<Cursor, RecordBatch>,
    tiles: TileMap,
}

#[derive(Debug, Clone)]
struct InMemCheckpoint {
    namespace: Vec<u8>,
    partitions: HashMap<PartitionKey, PartitionState>,
}

/// Reference backend. One read lock is the WRO snapshot boundary.
#[derive(Debug, Clone, Default)]
pub struct InMemWindowStore {
    partitions: Arc<RwLock<HashMap<PartitionKey, PartitionState>>>,
    checkpoints: Arc<Mutex<Vec<InMemCheckpoint>>>,
}

impl InMemWindowStore {
    pub fn new() -> Self {
        Self::default()
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
}

#[async_trait]
impl WindowOperatorStore for InMemWindowStore {
    async fn load_meta(&self, partition: &PartitionKey) -> Result<KeyState> {
        let partitions = self.partitions.read().await;
        Ok(partitions
            .get(partition)
            .map(|state| state.meta.clone())
            .unwrap_or_default())
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        let partitions = self.partitions.read().await;
        Ok(partitions
            .get(partition)
            .map(|state| Self::select_raw(state, runs))
            .unwrap_or_default())
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        let partitions = self.partitions.read().await;
        Ok(partitions
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
    ) -> Result<()> {
        let cursors = cursors_from_batch(events, ts_column_index)?;
        let mut partitions = self.partitions.write().await;
        let state = partitions.entry(partition.clone()).or_default();
        for (row, cursor) in cursors.into_iter().enumerate() {
            state.raw.insert(cursor, events.slice(row, 1));
        }
        for (key, value) in tiles {
            state.tiles.insert(*key, value.clone());
        }
        state.meta = meta.clone();
        Ok(())
    }

    async fn store_meta(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()> {
        let mut partitions = self.partitions.write().await;
        let state = partitions.entry(partition.clone()).or_default();
        state.meta = meta.clone();
        Ok(())
    }

    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowCheckpointMeta> {
        self.flush().await?;
        let snapshot = self
            .partitions
            .read()
            .await
            .iter()
            .filter(|(partition, _)| {
                partition.namespace.as_slice() == namespace.bytes.as_slice()
            })
            .map(|(partition, state)| (partition.clone(), state.clone()))
            .collect();
        let mut checkpoints = self.checkpoints.lock().await;
        let epoch = checkpoints.len() as u64;
        checkpoints.push(InMemCheckpoint {
            namespace: namespace.bytes.clone(),
            partitions: snapshot,
        });
        Ok(WindowCheckpointMeta {
            version: StateVersion {
                attempt: b"in-memory".to_vec(),
                epoch,
            },
        })
    }

    async fn restore(
        &self,
        namespace: &StateNamespace,
        meta: &WindowRestoreMeta,
    ) -> Result<()> {
        let version = &meta.base_version;
        anyhow::ensure!(
            version.attempt.as_slice() == b"in-memory",
            "checkpoint belongs to a different window store backend",
        );
        let checkpoint = self
            .checkpoints
            .lock()
            .await
            .get(version.epoch as usize)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("in-memory window checkpoint is unavailable"))?;
        anyhow::ensure!(
            checkpoint.namespace.as_slice() == namespace.bytes.as_slice(),
            "in-memory checkpoint namespace does not match runtime namespace",
        );
        let mut partitions = self.partitions.write().await;
        partitions.retain(|partition, _| {
            partition.namespace.as_slice() != namespace.bytes.as_slice()
        });
        partitions.extend(checkpoint.partitions);
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
        let partitions = self.partitions.read().await;
        let Some(state) = partitions.get(partition) else {
            return Ok(WindowData::new(Vec::new(), TileMap::new()));
        };
        Ok(WindowData::new(
            Self::select_raw(state, raw_runs),
            Self::select_tiles(state, tile_runs),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::state::tile::{
        TimeGranularity, TileRun, WindowTiles,
    };
    use crate::runtime::operators::window::store::row_nav::RowIdx;

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
        assert_eq!(actual.max_seen, expected.max_seen);
        assert_eq!(actual.processed_pos, expected.processed_pos);
        assert_eq!(actual.accumulators, expected.accumulators);
        assert_eq!(actual.first_ingested, expected.first_ingested);
        assert_eq!(actual.next_seq, expected.next_seq);
        assert_eq!(actual.retention_floor, expected.retention_floor);
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
            &store.load_meta(&partition).await.unwrap(),
            &KeyState::default(),
        );
        assert!(store.load_raw(&partition, &raw_runs).await.unwrap().is_empty());
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
            .store_meta(
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
            )
            .await
            .unwrap();

        let loaded = store
            .load_raw(
                &partition,
                &[
                    raw_run((10, 1), (30, 3)),
                    raw_run((20, 1), (40, 4)),
                ],
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
    async fn store_meta_leaves_raw_and_tiles_unchanged() {
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
            )
            .await
            .unwrap();
        let updated_meta = KeyState {
            max_seen: Some(Cursor::new(2_000, 5)),
            processed_pos: Some(Cursor::new(1_000, 4)),
            first_ingested: Some(Cursor::new(1_000, 4)),
            next_seq: 6,
            retention_floor: Some(Cursor::new(500, 0)),
            ..Default::default()
        };

        store.store_meta(&partition, &updated_meta).await.unwrap();

        assert_meta(&store.load_meta(&partition).await.unwrap(), &updated_meta);
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
            max_seen: Some(Cursor::new(2_000, 2)),
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
            )
            .await
            .unwrap();

        assert_meta(&store.load_meta(&partition).await.unwrap(), &meta);
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
}
