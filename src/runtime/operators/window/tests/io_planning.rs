use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::model::{Cursor, RawRun, TileRun, WindowTrigger};
use crate::runtime::operators::window::operator::WindowOperatorConfig;
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::store::{
    DueWorkStream, InMemWindowStore, KeyState, PartitionKey, StateNamespace, TileMap,
    WindowBackendSnapshot, WindowData, WindowOperatorStore, WindowRequestStore,
};
use std::any::Any;

use crate::runtime::state::{OperatorStore, OperatorTaskState};
use crate::test_utils::window::harness::{
    assert_window_values, batch, window_exec_from_sql, Harness, WoWroHarness,
};
use crate::runtime::operators::window::{TileConfig, TimeGranularity};

#[derive(Debug)]
struct RecordingWindowStore {
    inner: Arc<InMemWindowStore>,
    raw_reads: Mutex<Vec<Vec<RawRun>>>,
    tile_reads: Mutex<Vec<Vec<TileRun>>>,
    request_reads: Mutex<Vec<(Vec<RawRun>, Vec<TileRun>)>>,
}

impl RecordingWindowStore {
    fn new(inner: Arc<InMemWindowStore>) -> Self {
        Self {
            inner,
            raw_reads: Mutex::new(Vec::new()),
            tile_reads: Mutex::new(Vec::new()),
            request_reads: Mutex::new(Vec::new()),
        }
    }

    fn clear_reads(&self) {
        self.raw_reads.lock().unwrap().clear();
        self.tile_reads.lock().unwrap().clear();
        self.request_reads.lock().unwrap().clear();
    }
}

#[async_trait]
impl WindowOperatorStore for RecordingWindowStore {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState> {
        self.inner.load_key_state(partition).await
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        self.raw_reads.lock().unwrap().push(runs.to_vec());
        self.inner.load_raw(partition, runs).await
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        self.tile_reads.lock().unwrap().push(runs.to_vec());
        self.inner.load_tiles(partition, runs).await
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
        self.inner
            .commit_events(
                partition,
                ts_column_index,
                events,
                tiles,
                meta,
                triggers,
            )
            .await
    }

    fn stream_due<'a>(
        &'a self,
        namespace: &'a StateNamespace,
        after: Option<Cursor>,
        through: Cursor,
    ) -> DueWorkStream<'a> {
        self.inner.stream_due(namespace, after, through)
    }

    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()> {
        self.inner.store_key_state(partition, state).await
    }

    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowBackendSnapshot> {
        self.inner.checkpoint(namespace).await
    }

    async fn restore(
        &self,
        namespace: &StateNamespace,
        restore: &WindowBackendSnapshot,
    ) -> Result<()> {
        self.inner.restore(namespace, restore).await
    }
}

#[async_trait]
impl WindowRequestStore for RecordingWindowStore {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData> {
        self.request_reads
            .lock()
            .unwrap()
            .push((raw_runs.to_vec(), tile_runs.to_vec()));
        self.inner
            .load_window_data(partition, raw_runs, tile_runs)
            .await
    }
}

#[async_trait]
impl OperatorStore for RecordingWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics_labels(&self) -> Option<&crate::runtime::metrics::MetricsLabels> {
        self.inner.metrics_labels()
    }

    async fn maintain(&self, ns: &StateNamespace, state: &dyn OperatorTaskState) -> Result<()> {
        self.inner.maintain(ns, state).await
    }
}

async fn recording_harness(sql: &str) -> (Harness, Arc<RecordingWindowStore>) {
    let exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.spec = WindowSpec {
        tiling: Some(TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap()),
        ..WindowSpec::default()
    };
    let inner = Arc::new(InMemWindowStore::new());
    let recording = Arc::new(RecordingWindowStore::new(inner.clone()));
    let harness = Harness::with_operator_store(
        cfg,
        inner,
        recording.clone(),
        StateNamespace::new(b"io_planning"),
    )
    .await;
    (harness, recording)
}

#[tokio::test]
async fn wo_rebuild_loads_raw_edges_and_interior_tiles() {
    let sql = r#"SELECT timestamp, value, partition_key,
  MIN(value) OVER (
    PARTITION BY partition_key ORDER BY timestamp
    RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
  ) AS min_value
FROM test_table"#;
    let (mut h, recording) = recording_harness(sql).await;
    let initial_ts: Vec<_> = (0..=10).map(|minute| minute * 60_000).collect();
    h.ingest(
        batch(
            initial_ts.clone(),
            (0..=10).map(|value| value as f64).collect(),
            vec!["A"; initial_ts.len()],
        ),
        "A",
    )
    .await;
    let _ = h.watermark_and_output(600_000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert!(
        recording
            .tile_reads
            .lock()
            .unwrap()
            .last()
            .is_some_and(|runs| !runs.is_empty()),
        "cold rebuild must use tiles"
    );

    recording.clear_reads();
    h.ingest(batch(vec![660_000], vec![11.0], vec!["A"]), "A")
        .await;
    let out = h.watermark_and_output(660_000).await;
    assert_window_values(
        &out,
        3,
        &[vec![ScalarValue::Float64(Some(1.0))]],
        "tiled rebuild",
    );

    let raw_reads = recording.raw_reads.lock().unwrap();
    assert_eq!(raw_reads.len(), 1, "one merged emit and historical read");
    assert!(
        raw_reads[0]
            .iter()
            .all(|run| !(run.from.ts <= 300_000 && run.to.ts > 300_000)),
        "interior raw rows must not be loaded: {:?}",
        raw_reads[0]
    );
    let tile_reads = recording.tile_reads.lock().unwrap();
    assert!(
        tile_reads.last().is_some_and(|runs| !runs.is_empty()),
        "rebuild interior must use tiles"
    );
}

#[tokio::test]
async fn wo_warm_slide_plans_exact_tiled_leave_band() {
    let sql = r#"SELECT timestamp, value, partition_key,
  SUM(value) OVER (
    PARTITION BY partition_key ORDER BY timestamp
    RANGE BETWEEN INTERVAL '7' MINUTE PRECEDING AND CURRENT ROW
  ) AS sum_value
FROM test_table"#;
    let (mut h, recording) = recording_harness(sql).await;
    let initial_ts: Vec<_> = (0..=6).map(|minute| minute * 60_000).collect();
    h.ingest(
        batch(
            initial_ts.clone(),
            vec![1.0; initial_ts.len()],
            vec!["A"; initial_ts.len()],
        ),
        "A",
    )
    .await;
    let _ = h.watermark_and_output(360_000).await;
    let _ = h.drain_passthrough_watermark().await;

    recording.clear_reads();
    h.ingest(batch(vec![720_000], vec![10.0], vec!["A"]), "A")
        .await;
    let out = h.watermark_and_output(720_000).await;
    assert_window_values(
        &out,
        3,
        &[vec![ScalarValue::Float64(Some(12.0))]],
        "tiled slide",
    );

    let raw_reads = recording.raw_reads.lock().unwrap();
    assert_eq!(raw_reads.len(), 1, "one merged emit and leave-band read");
    assert!(
        raw_reads[0]
            .iter()
            .all(|run| !(run.from.ts <= 120_000 && run.to.ts > 120_000)),
        "leave-band interior raw rows must not be loaded: {:?}",
        raw_reads[0]
    );
    let tile_reads = recording.tile_reads.lock().unwrap();
    assert!(
        tile_reads.last().is_some_and(|runs| !runs.is_empty()),
        "large leave band must use tiles"
    );
}

#[tokio::test]
async fn wro_uses_one_selective_tiled_load() {
    let sql = r#"SELECT timestamp, value, partition_key,
  MIN(value) OVER (
    PARTITION BY partition_key ORDER BY timestamp
    RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
  ) AS min_value
FROM test_table"#;
    let tiling = TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap();
    let recording = Arc::new(RecordingWindowStore::new(Arc::new(InMemWindowStore::new())));
    let mut h = WoWroHarness::with_stores(
        sql,
        Some(tiling),
        true,
        recording.clone(),
        recording.clone(),
        StateNamespace::new(b"wro_io_planning"),
        0,
    )
    .await;
    let initial_ts: Vec<_> = (0..=10).map(|minute| minute * 60_000).collect();
    h.ingest(
        batch(
            initial_ts.clone(),
            (0..=10).map(|value| value as f64).collect(),
            vec!["A"; initial_ts.len()],
        ),
        "A",
    )
    .await;
    h.advance(600_000).await;

    recording.clear_reads();
    let out = h
        .request(batch(vec![660_000], vec![0.0], vec!["A"]), "A")
        .await;
    assert_window_values(
        &out,
        3,
        &[vec![ScalarValue::Float64(Some(1.0))]],
        "selective tiled WRO",
    );

    let request_reads = recording.request_reads.lock().unwrap();
    assert_eq!(request_reads.len(), 1, "one coherent WRO load");
    let (raw_runs, tile_runs) = &request_reads[0];
    assert!(
        raw_runs
            .iter()
            .all(|run| !(run.from.ts <= 300_000 && run.to.ts > 300_000)),
        "WRO interior raw rows must not be loaded: {raw_runs:?}"
    );
    assert!(!tile_runs.is_empty(), "WRO interior must use tiles");
}
