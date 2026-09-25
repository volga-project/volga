//! WRO reads: pin `window_kg_meta` per request, then filter data by `ReadOptions`.
//!
//! Given a session — never `connect()`. No key-group range, no lifecycle.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::OnceCell;

use crate::runtime::metrics::MetricsLabels;
use crate::runtime::operators::window::model::{PartitionKey, RawRun, TileMap, TileRun};
use crate::runtime::operators::window::store::backend::{
    ReadOptions, Version, WindowRead, WindowRequestStore,
};
use crate::runtime::operators::window::store::WindowData;

use super::cql::{mark_idempotent, prepare_stmts, SELECT_META, SELECT_RAW, SELECT_TILES};
use super::meta::{self, MetaRow};
use super::observe::{self, CallMeter};
use super::read::{fetch_raw, fetch_tiles};
use super::schema::TABLES;

struct PreparedRequest {
    select_meta: PreparedStatement,
    select_raw: PreparedStatement,
    select_tiles: PreparedStatement,
}

#[derive(Clone)]
pub struct ScyllaWindowRequestStore {
    session: Arc<Session>,
    max_parallelism: usize,
    prepared: Arc<OnceCell<PreparedRequest>>,
    metrics_task_id: Option<String>,
    metrics_labels: Option<MetricsLabels>,
}

impl std::fmt::Debug for ScyllaWindowRequestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScyllaWindowRequestStore")
            .field("max_parallelism", &self.max_parallelism)
            .finish()
    }
}

impl ScyllaWindowRequestStore {
    pub fn from_session(session: Arc<Session>, max_parallelism: usize) -> Self {
        Self {
            session,
            max_parallelism: max_parallelism.max(1),
            prepared: Arc::new(OnceCell::new()),
            metrics_task_id: None,
            metrics_labels: None,
        }
    }

    pub fn with_metrics(mut self, task_id: String, labels: MetricsLabels) -> Self {
        self.metrics_task_id = Some(task_id);
        self.metrics_labels = Some(labels);
        self
    }

    fn call_meter(&self) -> CallMeter {
        CallMeter {
            task_id: self.metrics_task_id.clone().unwrap_or_default(),
            labels: self.metrics_labels.clone(),
        }
    }

    async fn prepared(&self) -> Result<&PreparedRequest> {
        self.prepared
            .get_or_try_init(|| async {
                let session = Arc::clone(&self.session);
                futures::future::try_join_all(
                    TABLES
                        .iter()
                        .map(|table| session.query_unpaged(*table, &[])),
                )
                .await?;
                let [select_meta, select_raw, select_tiles] =
                    prepare_stmts(session.as_ref(), [SELECT_META, SELECT_RAW, SELECT_TILES])
                        .await?;
                Ok::<_, anyhow::Error>(PreparedRequest {
                    select_meta: mark_idempotent(select_meta),
                    select_raw: mark_idempotent(select_raw),
                    select_tiles: mark_idempotent(select_tiles),
                })
            })
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    fn key_group(&self, partition: &PartitionKey) -> i32 {
        partition.key_group(self.max_parallelism) as i32
    }

    async fn load_pin(&self, partition: &PartitionKey, kg: i32) -> Result<Option<MetaRow>> {
        let prepared = self.prepared().await?;
        meta::load_row(
            self.session.as_ref(),
            &prepared.select_meta,
            partition.namespace.as_slice(),
            kg,
        )
        .await
    }

    async fn load_filtered(
        &self,
        partition: &PartitionKey,
        kg: i32,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
        raw_visible: impl Fn(Version, i64) -> bool + Send + Sync,
        tile_visible: impl Fn(Version) -> bool + Send + Sync,
    ) -> Result<(Vec<arrow::array::RecordBatch>, TileMap)> {
        let prepared = self.prepared().await?;
        let session = Arc::clone(&self.session);
        let ns = partition.namespace.clone();
        let key = partition.business_key.clone();
        let (raw, tiles) = tokio::try_join!(
            fetch_raw(
                Arc::clone(&session),
                prepared.select_raw.clone(),
                ns.clone(),
                kg,
                key.clone(),
                raw_runs,
                raw_visible,
            ),
            fetch_tiles(
                session,
                prepared.select_tiles.clone(),
                ns,
                kg,
                key,
                tile_runs,
                tile_visible,
            ),
        )?;
        Ok((raw, tiles))
    }

    async fn check_stale(&self, partition: &PartitionKey, kg: i32, pin: &MetaRow) -> Result<()> {
        let Some(p) = pin.pinned_checkpoint() else {
            return Ok(());
        };
        let Some(again) = self.load_pin(partition, kg).await? else {
            anyhow::bail!("window_kg_meta disappeared during read");
        };
        if let Some(prev) = again.pinned_prev_checkpoint() {
            anyhow::ensure!(
                p >= prev,
                "request pinned checkpoint {p} is stale (prev_checkpoint_id={prev})"
            );
        }
        Ok(())
    }
}

fn coverage_lo(raw_runs: &[RawRun], tile_runs: &[TileRun]) -> Option<i64> {
    raw_runs
        .iter()
        .map(|r| r.from.ts)
        .chain(tile_runs.iter().map(|r| r.start_ts))
        .min()
}

fn clip_tiles_committed(runs: &[TileRun], h: i64) -> Vec<TileRun> {
    let end = h.saturating_add(1);
    runs.iter()
        .filter_map(|r| {
            if r.start_ts >= end {
                None
            } else if r.end_ts_exclusive > end {
                Some(TileRun {
                    granularity: r.granularity,
                    start_ts: r.start_ts,
                    end_ts_exclusive: end,
                })
            } else {
                Some(r.clone())
            }
        })
        .collect()
}

#[async_trait]
impl WindowRequestStore for ScyllaWindowRequestStore {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
        opts: ReadOptions,
    ) -> Result<WindowRead> {
        let meter = self.call_meter();
        observe::observe(
            &meter,
            "load_window_data",
            self.load_window_data_inner(partition, raw_runs, tile_runs, opts),
        )
        .await
    }
}

impl ScyllaWindowRequestStore {
    async fn load_window_data_inner(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
        opts: ReadOptions,
    ) -> Result<WindowRead> {
        let kg = self.key_group(partition);
        let Some(pin) = self.load_pin(partition, kg).await? else {
            return Ok(WindowRead::empty());
        };
        if let (Some(floor), Some(lo)) = (pin.retention_floor, coverage_lo(raw_runs, tile_runs)) {
            anyhow::ensure!(
                lo >= floor,
                "request range is no longer retained (lo={lo} < retention_floor={floor})"
            );
        }
        if pin.cut.entries().is_empty() {
            return Ok(WindowRead {
                data: WindowData::new(Vec::new(), TileMap::new()),
                committed_wm: pin.committed_wm,
                checkpoint_id: pin.pinned_checkpoint(),
                retention_floor: pin.retention_floor,
            });
        }

        let cut = pin.cut.clone();
        let (raw, tiles) = match (opts, pin.committed_wm) {
            (ReadOptions::Fresh, Some(h)) => {
                let tiles = clip_tiles_committed(tile_runs, h);
                let cur = pin.attempt();
                let cut_raw = cut.clone();
                let cut_tiles = cut;
                self.load_filtered(
                    partition,
                    kg,
                    raw_runs,
                    &tiles,
                    move |v, ts| cut_raw.allows(v) || (v.attempt == cur && ts > h),
                    move |v| cut_tiles.allows(v),
                )
                .await?
            }
            _ => {
                let cut_raw = cut.clone();
                let cut_tiles = cut;
                self.load_filtered(
                    partition,
                    kg,
                    raw_runs,
                    tile_runs,
                    move |v, _| cut_raw.allows(v),
                    move |v| cut_tiles.allows(v),
                )
                .await?
            }
        };

        self.check_stale(partition, kg, &pin).await?;

        Ok(WindowRead {
            data: WindowData::new(raw, tiles),
            committed_wm: pin.committed_wm,
            checkpoint_id: pin.pinned_checkpoint(),
            retention_floor: pin.retention_floor,
        })
    }
}
