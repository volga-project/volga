//! Systematic `WindowOperator` test suite (new-style).
//!
//! Goals:
//! - Keep each test self-contained and readable.
//! - Use one consistent structure everywhere: **ingest → watermark → assert → drain watermark passthrough**.
//! - Cover a bounded “matrix” of scenarios (window kind × agg kind × lateness) plus targeted edge-cases:
//!   - watermark-before-data
//!   - terminal (MAX) watermark flush
//!   - multi-window queries
//!   - tiling
//!   - pruning state checks
//!
//! Terminology:
//! - **gap cursor**: when `(watermark_ts - lateness_ms)` falls *between* two event-time rows
//!   (e.g. 17000ms between rows at 15000ms and 20000ms). Without clamping to an existing row,
//!   retractable aggregations can become inconsistent across advances.
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::prelude::SessionContext;

use crate::api::planner::{Planner, PlanningContext};
use crate::common::message::Message;
use crate::common::{MAX_WATERMARK_VALUE, WatermarkMessage};
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::window_operator::{
    ExecutionMode, RequestAdvancePolicy, WindowOperator, WindowOperatorConfig,
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::OperatorStates;
use crate::storage::batch_store::{BatchStore, InMemBatchStore};
use crate::storage::{StorageBudgetConfig, WorkerStorageContext};
use crate::common::Key;
use crate::runtime::operators::window::TimeGranularity;

fn test_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

fn batch(ts: Vec<i64>, vals: Vec<f64>, keys: Vec<&str>) -> RecordBatch {
    let schema = test_input_schema();
    let ts = Arc::new(TimestampMillisecondArray::from(ts));
    let vals = Arc::new(Float64Array::from(vals));
    let keys = Arc::new(StringArray::from(keys));
    RecordBatch::try_new(schema, vec![ts, vals, keys]).expect("test batch")
}

fn key(partition: &str) -> Key {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition",
        DataType::Utf8,
        false,
    )]));
    let arr = StringArray::from(vec![partition]);
    let key_batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("key batch");
    Key::new(key_batch).expect("key")
}

fn keyed_message(b: RecordBatch, partition: &str) -> Message {
    Message::new_keyed(None, b, key(partition), None, None)
}

fn watermark_message(wm: u64) -> Message {
    Message::Watermark(WatermarkMessage::new("test".to_string(), wm, Some(0)))
}

async fn window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    let schema = test_input_schema();
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        schema,
    );
    extract_datafusion_window_exec(sql, &mut planner).await
}

fn runtime_context() -> RuntimeContext {
    let store = Arc::new(InMemBatchStore::new(64, TimeGranularity::Seconds(1), 128))
        as Arc<dyn BatchStore>;
    let shared =
        WorkerStorageContext::new(store, StorageBudgetConfig::default()).expect("storage ctx");

    let mut ctx = RuntimeContext::new(
        "test_vertex".to_string().into(),
        0,
        1,
        None,
        Some(Arc::new(OperatorStates::new())),
        None,
    );
    ctx.set_worker_storage_context(shared);
    ctx
}

struct Harness {
    op: WindowOperator,
    mode: ExecutionMode,
}

impl Harness {
    async fn new(cfg: WindowOperatorConfig) -> Self {
        let ctx = runtime_context();
        let mode = cfg.execution_mode.clone();
        let mut op = WindowOperator::new(OperatorConfig::WindowConfig(cfg));
        op.open(&ctx).await.expect("open");
        Self { op, mode }
    }

    async fn ingest(&mut self, b: RecordBatch, partition: &str) {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            keyed_message(b, partition),
        ]))));
        assert!(matches!(
            self.op.poll_next().await,
            OperatorPollResult::Continue
        ));
    }

    async fn watermark_and_maybe_output(&mut self, wm: u64) -> Option<RecordBatch> {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            watermark_message(wm),
        ]))));

        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Regular(base)) => Some(base.record_batch),
            OperatorPollResult::Continue => None,
            other => panic!("unexpected poll result on watermark: {:?}", other),
        }
    }

    async fn drain_passthrough_watermark(&mut self) -> u64 {
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Watermark(wm)) => wm.watermark_value,
            other => panic!("expected passthrough watermark, got {:?}", other),
        }
    }

    async fn close(mut self) {
        self.op.close().await.expect("close");
    }
}

fn col_f64(b: &RecordBatch, idx: usize) -> &Float64Array {
    b.column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64 col")
}
fn col_i64(b: &RecordBatch, idx: usize) -> &Int64Array {
    b.column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 col")
}

#[tokio::test]
async fn test_range_mixed_aggs_emits_on_watermark_across_steps() {
    // Covers: RANGE + mixed agg types + buffering across multiple watermarks.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_val,
        COUNT(value) OVER w as count_val,
        AVG(value) OVER w as avg_val,
        MIN(value) OVER w as min_val,
        MAX(value) OVER w as max_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    let mut h = Harness::new(cfg).await;

    h.ingest(batch(vec![1000], vec![10.0], vec!["A"]), "A").await;
    let out1 = h
        .watermark_and_maybe_output(1000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 1);
    assert_eq!(col_f64(&out1, 3).value(0), 10.0);
    assert_eq!(col_i64(&out1, 4).value(0), 1);
    assert_eq!(h.drain_passthrough_watermark().await, 1000);

    h.ingest(batch(vec![1500, 2000], vec![30.0, 20.0], vec!["A", "A"]), "A")
        .await;
    let out2 = h
        .watermark_and_maybe_output(2000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 2);
    assert_eq!(col_f64(&out2, 3).value(0), 40.0);
    assert_eq!(col_i64(&out2, 4).value(0), 2);
    assert_eq!(col_f64(&out2, 3).value(1), 60.0);
    assert_eq!(col_i64(&out2, 4).value(1), 3);
    assert_eq!(h.drain_passthrough_watermark().await, 2000);

    h.ingest(batch(vec![3200], vec![5.0], vec!["A"]), "A").await;
    let out3 = h
        .watermark_and_maybe_output(3200)
        .await
        .expect("regular output");
    assert_eq!(out3.num_rows(), 1);
    assert_eq!(col_f64(&out3, 3).value(0), 55.0);
    assert_eq!(col_i64(&out3, 4).value(0), 3);
    assert_eq!(h.drain_passthrough_watermark().await, 3200);

    h.ingest(
        batch(vec![3500, 4000], vec![100.0, 200.0], vec!["B", "B"]),
        "B",
    )
    .await;
    let out4 = h
        .watermark_and_maybe_output(4000)
        .await
        .expect("regular output");
    assert_eq!(out4.num_rows(), 2);
    assert_eq!(col_f64(&out4, 3).value(0), 100.0);
    assert_eq!(col_f64(&out4, 3).value(1), 300.0);
    assert_eq!(h.drain_passthrough_watermark().await, 4000);

    h.close().await;
}

#[tokio::test]
async fn test_rows_sum_3rows_emits_on_watermark() {
    // Covers: ROWS window basic semantics.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_3_rows
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(
            vec![1000, 2000, 3000, 4000, 5000],
            vec![10.0, 20.0, 30.0, 40.0, 50.0],
            vec!["test", "test", "test", "test", "test"],
        ),
        "test",
    )
    .await;
    let out = h
        .watermark_and_maybe_output(6000)
        .await
        .expect("regular output");
    assert_eq!(out.num_rows(), 5);
    let sum = col_f64(&out, 3);
    assert_eq!(sum.value(0), 10.0);
    assert_eq!(sum.value(1), 30.0);
    assert_eq!(sum.value(2), 60.0);
    assert_eq!(sum.value(3), 90.0);
    assert_eq!(sum.value(4), 120.0);
    assert_eq!(h.drain_passthrough_watermark().await, 6000);

    h.close().await;
}

#[tokio::test]
async fn test_multi_window_sizes_and_strict_late_drop_on_ingest() {
    // Covers: multiple RANGE windows with different widths + late drop on ingest.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w1 as sum_small,
        AVG(value) OVER w2 as avg_large
      FROM test_table
      WINDOW
        w1 AS (PARTITION BY partition_key ORDER BY timestamp
          RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY partition_key ORDER BY timestamp
          RANGE BETWEEN INTERVAL '3000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(
            vec![1000, 2000, 3000, 4000, 5000],
            vec![10.0, 20.0, 30.0, 40.0, 50.0],
            vec!["A", "A", "A", "A", "A"],
        ),
        "A",
    )
    .await;
    let out1 = h
        .watermark_and_maybe_output(5000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 5);
    assert_eq!(h.drain_passthrough_watermark().await, 5000);

    // Late rows (1500, 2500) relative to watermark=5000 should be dropped on ingest.
    h.ingest(
        batch(vec![1500, 6000, 2500], vec![15.0, 60.0, 25.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out2 = h
        .watermark_and_maybe_output(6000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 1);
    let sum_small = col_f64(&out2, 3);
    let avg_large = col_f64(&out2, 4);
    assert_eq!(sum_small.value(0), 110.0);
    assert_eq!(avg_large.value(0), 45.0);
    assert_eq!(h.drain_passthrough_watermark().await, 6000);

    h.close().await;
}

#[tokio::test]
async fn test_tiling_min_max_and_avg_smoke_and_strict_late_drop() {
    // Covers: tiling path + mixed aggregates + late drop on ingest.
    let sql = "SELECT
        timestamp, value, partition_key,
        MIN(value) OVER w as min_val,
        MAX(value) OVER w as max_val,
        AVG(value) OVER w as avg_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;

    use crate::runtime::operators::window::state::tiles::{TileConfig, TimeGranularity as Tg};
    let tile_config = TileConfig::new(vec![Tg::Minutes(1), Tg::Minutes(5)]).expect("tile cfg");
    cfg.tiling_configs = vec![Some(tile_config.clone()), Some(tile_config.clone()), Some(tile_config)];

    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(
            vec![60000, 180000, 300000, 420000],
            vec![10.0, 30.0, 50.0, 70.0],
            vec!["A", "A", "A", "A"],
        ),
        "A",
    )
    .await;
    let out1 = h
        .watermark_and_maybe_output(420000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 4);
    assert_eq!(col_f64(&out1, 3).value(0), 10.0);
    assert_eq!(col_f64(&out1, 4).value(0), 10.0);
    assert_eq!(col_f64(&out1, 5).value(0), 10.0);
    assert_eq!(h.drain_passthrough_watermark().await, 420000);

    // Late relative to 420000: [60000,120000,180000] should be dropped on ingest.
    h.ingest(
        batch(vec![60000, 120000, 180000], vec![5.0, 15.0, 25.0], vec!["B", "B", "B"]),
        "B",
    )
    .await;
    let out2 = h
        .watermark_and_maybe_output(420000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 0);
    assert_eq!(h.drain_passthrough_watermark().await, 420000);

    h.close().await;
}

#[tokio::test]
async fn test_gap_cursor_range_watermark_minus_lateness_between_rows_mixed_aggs() {
    // Covers: gap cursor (wm - lateness between rows) for RANGE, mixed retractable + plain.
    let sql = "SELECT
        timestamp, value, partition_key,
        AVG(value) OVER w as avg_val,
        MIN(value) OVER w as min_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    cfg.spec.lateness = Some(3000);
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(vec![10000, 15000, 20000], vec![100.0, 150.0, 200.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;

    let out1 = h
        .watermark_and_maybe_output(20000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 2);
    assert_eq!(col_f64(&out1, 3).value(0), 100.0);
    assert_eq!(col_f64(&out1, 4).value(0), 100.0);
    assert_eq!(col_f64(&out1, 3).value(1), 125.0);
    assert_eq!(col_f64(&out1, 4).value(1), 100.0);
    assert_eq!(h.drain_passthrough_watermark().await, 20000);

    let out2 = h
        .watermark_and_maybe_output(25000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 1);
    assert_eq!(col_f64(&out2, 3).value(0), 150.0);
    assert_eq!(col_f64(&out2, 4).value(0), 100.0);
    assert_eq!(h.drain_passthrough_watermark().await, 25000);

    h.close().await;
}

#[tokio::test]
async fn test_gap_cursor_rows_watermark_minus_lateness_between_rows_mixed_aggs() {
    // Covers: gap cursor for ROWS, mixed retractable + plain.
    let sql = "SELECT
        timestamp, value, partition_key,
        AVG(value) OVER w as avg_val,
        MAX(value) OVER w as max_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    cfg.spec.lateness = Some(3000);
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(vec![10000, 15000, 20000], vec![100.0, 150.0, 200.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;

    let out1 = h
        .watermark_and_maybe_output(20000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 2);
    assert_eq!(col_f64(&out1, 3).value(0), 100.0);
    assert_eq!(col_f64(&out1, 4).value(0), 100.0);
    assert_eq!(col_f64(&out1, 3).value(1), 125.0);
    assert_eq!(col_f64(&out1, 4).value(1), 150.0);
    assert_eq!(h.drain_passthrough_watermark().await, 20000);

    let out2 = h
        .watermark_and_maybe_output(25000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 1);
    assert_eq!(col_f64(&out2, 3).value(0), 150.0);
    assert_eq!(col_f64(&out2, 4).value(0), 200.0);
    assert_eq!(h.drain_passthrough_watermark().await, 25000);

    h.close().await;
}

#[tokio::test]
async fn test_watermark_before_any_data_is_noop_and_does_not_panic() {
    // Covers: watermark arriving before any data for a key.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = false;
    let mut h = Harness::new(cfg).await;

    let out0 = h
        .watermark_and_maybe_output(1000)
        .await
        .expect("regular output");
    assert_eq!(out0.num_rows(), 0);
    assert_eq!(h.drain_passthrough_watermark().await, 1000);

    // Now ingest late data relative to watermark: should be dropped on ingest, still no output.
    h.ingest(batch(vec![500], vec![1.0], vec!["A"]), "A").await;
    let out1 = h
        .watermark_and_maybe_output(1000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 0);
    assert_eq!(h.drain_passthrough_watermark().await, 1000);

    h.close().await;
}

#[tokio::test]
async fn test_late_within_lateness_is_kept_on_ingest() {
    // Late row arrives after watermark but within lateness; should be accepted and emitted later.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = false;
    cfg.spec.lateness = Some(5000);
    let mut h = Harness::new(cfg).await;

    // Advance watermark with no data; output batch is empty.
    let out0 = h
        .watermark_and_maybe_output(20000)
        .await
        .expect("regular output");
    assert_eq!(out0.num_rows(), 0);
    assert_eq!(h.drain_passthrough_watermark().await, 20000);

    // Late row at 17000 (> 20000 - 5000) should be accepted on ingest.
    h.ingest(batch(vec![17000], vec![1.0], vec!["A"]), "A").await;

    // Next watermark advances past 17000; row should be emitted.
    let out1 = h
        .watermark_and_maybe_output(25000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 1);
    let ts = out1
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts col");
    assert_eq!(ts.value(0), 17000);
    assert_eq!(h.drain_passthrough_watermark().await, 25000);

    h.close().await;
}

#[tokio::test]
async fn test_terminal_max_watermark_flushes_all_buffered_rows_even_with_lateness() {
    // Covers: terminal watermark flush semantics (lateness should not block MAX).
    let sql = "SELECT
        timestamp, value, partition_key,
        COUNT(value) OVER w as cnt
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = true;
    cfg.spec.lateness = Some(3000);
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(vec![10000, 15000, 20000], vec![1.0, 1.0, 1.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;

    let out = h
        .watermark_and_maybe_output(MAX_WATERMARK_VALUE)
        .await
        .expect("regular output");
    assert_eq!(out.num_rows(), 3);
    assert_eq!(h.drain_passthrough_watermark().await, MAX_WATERMARK_VALUE);

    h.close().await;
}

#[tokio::test]
async fn test_request_mode_emits_no_regular_batches_but_passthroughs_watermarks() {
    // Covers: ExecutionMode::Request path inside WindowOperator (no output on watermark).
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.execution_mode = ExecutionMode::Request;
    cfg.spec.request_advance_policy = RequestAdvancePolicy::OnWatermark;
    cfg.spec.parallelize = false;

    let mut h = Harness::new(cfg).await;
    assert!(matches!(h.mode, ExecutionMode::Request));

    h.ingest(batch(vec![1000], vec![10.0], vec!["A"]), "A").await;
    assert!(h.watermark_and_maybe_output(1000).await.is_none());
    assert_eq!(h.drain_passthrough_watermark().await, 1000);

    h.close().await;
}

#[tokio::test]
async fn test_matrix_smoke_window_kind_x_agg_kind_x_lateness() {
    // Compact systematic matrix:
    // - window kind: RANGE vs ROWS
    // - agg kind: plain (MIN/MAX) vs retractable (AVG)
    // - lateness: off vs on (gap cursor via watermark=20000, lateness=3000)
    //
    // Each case asserts only a couple of high-signal invariants (row count + one value),
    // keeping failures easy to localize.
    struct Case {
        name: &'static str,
        sql: &'static str,
        lateness_ms: Option<i64>,
        wm: u64,
        expected_rows: usize,
        expected_last_value: f64,
        value_col_idx: usize,
    }

    let cases = vec![
        // RANGE + AVG (retractable)
        Case {
            name: "range_avg_no_lateness",
            sql: "SELECT timestamp, value, partition_key, AVG(value) OVER w as avg_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)",
            lateness_ms: None,
            wm: 20000,
            expected_rows: 3,
            expected_last_value: 150.0,
            value_col_idx: 3,
        },
        Case {
            name: "range_avg_with_lateness_gap_cursor",
            sql: "SELECT timestamp, value, partition_key, AVG(value) OVER w as avg_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)",
            lateness_ms: Some(3000),
            wm: 20000,
            expected_rows: 2,
            expected_last_value: 125.0,
            value_col_idx: 3,
        },
        // RANGE + MIN (plain)
        Case {
            name: "range_min_no_lateness",
            sql: "SELECT timestamp, value, partition_key, MIN(value) OVER w as min_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)",
            lateness_ms: None,
            wm: 20000,
            expected_rows: 3,
            expected_last_value: 100.0,
            value_col_idx: 3,
        },
        Case {
            name: "range_min_with_lateness_gap_cursor",
            sql: "SELECT timestamp, value, partition_key, MIN(value) OVER w as min_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)",
            lateness_ms: Some(3000),
            wm: 20000,
            expected_rows: 2,
            expected_last_value: 100.0,
            value_col_idx: 3,
        },
        // ROWS + AVG (retractable)
        Case {
            name: "rows_avg_no_lateness",
            sql: "SELECT timestamp, value, partition_key, AVG(value) OVER w as avg_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            lateness_ms: None,
            wm: 20000,
            expected_rows: 3,
            expected_last_value: 150.0,
            value_col_idx: 3,
        },
        Case {
            name: "rows_avg_with_lateness_gap_cursor",
            sql: "SELECT timestamp, value, partition_key, AVG(value) OVER w as avg_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            lateness_ms: Some(3000),
            wm: 20000,
            expected_rows: 2,
            expected_last_value: 125.0,
            value_col_idx: 3,
        },
        // ROWS + MAX (plain)
        Case {
            name: "rows_max_no_lateness",
            sql: "SELECT timestamp, value, partition_key, MAX(value) OVER w as max_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            lateness_ms: None,
            wm: 20000,
            expected_rows: 3,
            expected_last_value: 200.0,
            value_col_idx: 3,
        },
        Case {
            name: "rows_max_with_lateness_gap_cursor",
            sql: "SELECT timestamp, value, partition_key, MAX(value) OVER w as max_val
                  FROM test_table
                  WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            lateness_ms: Some(3000),
            wm: 20000,
            expected_rows: 2,
            expected_last_value: 150.0,
            value_col_idx: 3,
        },
    ];

    for c in cases {
        let window_exec = window_exec_from_sql(c.sql).await;
        let mut cfg = WindowOperatorConfig::new(window_exec);
        cfg.spec.parallelize = false;
        cfg.spec.lateness = c.lateness_ms;
        let mut h = Harness::new(cfg).await;

        h.ingest(
            batch(vec![10000, 15000, 20000], vec![100.0, 150.0, 200.0], vec!["A", "A", "A"]),
            "A",
        )
        .await;
        let out = h
            .watermark_and_maybe_output(c.wm)
            .await
            .unwrap_or_else(|| panic!("{}: expected regular output batch", c.name));

        assert_eq!(out.num_rows(), c.expected_rows, "{}: row count", c.name);
        assert_eq!(
            col_f64(&out, c.value_col_idx).value(out.num_rows() - 1),
            c.expected_last_value,
            "{}: last value mismatch",
            c.name
        );

        assert_eq!(
            h.drain_passthrough_watermark().await,
            c.wm,
            "{}: passthrough watermark",
            c.name
        );
        h.close().await;
    }
}

#[tokio::test]
async fn test_multi_window_mixed_aggs_correctness_smoke() {
    // Complex query shape (multi-window, mixed agg kinds) correctness smoke.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w1 as sum_2s,
        AVG(value) OVER w2 as avg_5s,
        MIN(value) OVER w2 as min_5s,
        COUNT(value) OVER w3 as count_3rows,
        MAX(value) OVER w3 as max_3rows
      FROM test_table
      WINDOW
        w1 AS (PARTITION BY partition_key ORDER BY timestamp
          RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY partition_key ORDER BY timestamp
          RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW),
        w3 AS (PARTITION BY partition_key ORDER BY timestamp
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = false;
    cfg.spec.lateness = Some(3000);
    let mut h = Harness::new(cfg).await;

    // Step 1: wm=20000 => advance_to=17000 => emits [10000,15000]
    h.ingest(
        batch(vec![10000, 15000, 20000], vec![100.0, 150.0, 200.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out1 = h
        .watermark_and_maybe_output(20000)
        .await
        .expect("regular output");
    assert_eq!(out1.num_rows(), 2);
    // last emitted row is ts=15000
    assert_eq!(col_f64(&out1, 3).value(1), 150.0);
    assert_eq!(col_f64(&out1, 4).value(1), 125.0);
    assert_eq!(col_f64(&out1, 5).value(1), 100.0);
    assert_eq!(col_i64(&out1, 6).value(1), 2);
    assert_eq!(col_f64(&out1, 7).value(1), 150.0);
    assert_eq!(h.drain_passthrough_watermark().await, 20000);

    // Step 2: wm=25000 => advance_to=22000 => emits [18000, 20000]
    h.ingest(
        batch(vec![16000, 18000, 25000], vec![160.0, 180.0, 250.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out2 = h
        .watermark_and_maybe_output(25000)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 2);
    // Row 0 (ts=18000)
    assert_eq!(col_f64(&out2, 3).value(0), 180.0); // sum_2s
    assert!((col_f64(&out2, 4).value(0) - 165.0).abs() < 1e-9); // avg_5s
    assert_eq!(col_f64(&out2, 5).value(0), 150.0); // min_5s
    assert_eq!(col_i64(&out2, 6).value(0), 3); // count_3rows
    assert_eq!(col_f64(&out2, 7).value(0), 180.0); // max_3rows
    // Row 1 (ts=20000)
    assert_eq!(col_f64(&out2, 3).value(1), 380.0); // sum_2s
    assert!((col_f64(&out2, 4).value(1) - 176.66666666666666).abs() < 1e-9); // avg_5s
    assert_eq!(col_f64(&out2, 5).value(1), 150.0); // min_5s
    assert_eq!(col_i64(&out2, 6).value(1), 3); // count_3rows
    assert_eq!(col_f64(&out2, 7).value(1), 200.0); // max_3rows
    assert_eq!(h.drain_passthrough_watermark().await, 25000);

    h.close().await;
}

#[tokio::test]
async fn test_watermark_inside_single_run_advances_and_emits() {
    // Regression for the limitation of metadata-only clamping: if the watermark boundary falls
    // *inside* a single in-mem run, we still must advance and emit rows <= watermark.
    //
    // This test ensures we don't "stall" until the watermark passes the run's max_pos.
    let sql = "SELECT
        timestamp, value, partition_key,
        COUNT(value) OVER w as cnt
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '100000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = false;
    cfg.spec.lateness = None;
    let mut h = Harness::new(cfg).await;

    // One big batch in a single bucket.
    let ts: Vec<i64> = (1000..2000).collect(); // 1000 rows
    h.ingest(batch(ts, vec![1.0; 1000], vec!["A"; 1000]), "A")
        .await;

    // Watermark falls inside the run.
    let out = h
        .watermark_and_maybe_output(1700)
        .await
        .expect("regular output");
    assert_eq!(out.num_rows(), 701);
    let cnt = col_i64(&out, 3);
    assert_eq!(cnt.value(out.num_rows() - 1), 701);
    assert_eq!(h.drain_passthrough_watermark().await, 1700);

    // Advance further; remaining rows should be emitted.
    let out2 = h
        .watermark_and_maybe_output(1999)
        .await
        .expect("regular output");
    assert_eq!(out2.num_rows(), 299);
    assert_eq!(h.drain_passthrough_watermark().await, 1999);

    h.close().await;
}

async fn run_range_window_reference_model(batch_size: usize) {
    // Reproduces the e2e serial failure in a pure operator-level test:
    // - one key
    // - timestamps are strictly increasing by 1ms
    // - watermarks advance in coarse steps (simulating batch-based watermark injection)
    // - lateness is non-zero (so `(wm - lateness)` lands between rows most of the time)
    //
    // Correctness requirement: output count for *every* timestamp matches the reference model:
    // `cnt(ts) = min(ts - start_ts + 1, window_len_ms + 1)` for monotonically increasing 1ms timestamps.
    let sql = "SELECT
        timestamp, value, partition_key,
        SUM(value) OVER w as sum_val,
        COUNT(value) OVER w as cnt_val,
        AVG(value) OVER w as avg_val
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    // Sanity: ensure we parsed the window length we think we did.
    let window_frame = window_exec.window_expr()[0].get_window_frame();
    let wl = crate::storage::index::get_window_length_ms(window_frame);
    assert_eq!(wl, 2000, "expected window length to be 2000ms");
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.spec.parallelize = false;
    cfg.spec.lateness = Some(250);

    let mut h = Harness::new(cfg).await;

    let total_records: usize = 3000;
    let start_ts: i64 = 1000;
    let batch_size: usize = batch_size;

    // Collect output rows as (ts, cnt).
    let mut seen: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();

    let mut next = 0usize;
    while next < total_records {
        let end = (next + batch_size).min(total_records);
        let ts: Vec<i64> = (start_ts + next as i64..start_ts + end as i64).collect();
        let vals: Vec<f64> = (0..(end - next))
            .map(|i| if (next + i) % 2 == 0 { 1.0 } else { 2.0 })
            .collect();
        let keys: Vec<&str> = vec!["A"; end - next];
        let b = batch(ts.clone(), vals, keys);

        h.ingest(b, "A").await;

        let wm = *ts.last().expect("non-empty batch") as u64;
        let out = h
            .watermark_and_maybe_output(wm)
            .await
            .expect("regular output");
        assert_eq!(h.drain_passthrough_watermark().await, wm);

        if out.num_rows() > 0 {
            let out_ts = out
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts col");
            let out_cnt = col_i64(&out, 4);
            for i in 0..out.num_rows() {
                seen.insert(out_ts.value(i), out_cnt.value(i));
            }
        }

        next = end;
    }

    // Flush remaining buffered rows.
    let out = h
        .watermark_and_maybe_output(MAX_WATERMARK_VALUE)
        .await
        .expect("regular output");
    assert_eq!(
        h.drain_passthrough_watermark().await,
        MAX_WATERMARK_VALUE
    );
    if out.num_rows() > 0 {
        let out_ts = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts col");
        let out_cnt = col_i64(&out, 4);
        for i in 0..out.num_rows() {
            seen.insert(out_ts.value(i), out_cnt.value(i));
        }
    }

    assert_eq!(
        seen.len(),
        total_records,
        "expected one output row per input row (batch_size={})",
        batch_size
    );
    for i in 0..total_records {
        let ts = start_ts + (i as i64);
        let expected = ((i + 1) as i64).min(2001);
        let got = *seen.get(&ts).unwrap_or_else(|| panic!("missing ts={} (batch_size={})", ts, batch_size));
        assert_eq!(
            got, expected,
            "cnt mismatch for ts={} (batch_size={})",
            ts, batch_size
        );
    }

    h.close().await;
}

#[tokio::test]
async fn test_range_window_reference_model_high_frequency_watermarks() {
    // Matches the e2e cadence: `batch_size=256` => watermark ticks every 256 rows.
    run_range_window_reference_model(256).await;
}

#[tokio::test]
async fn test_range_window_reference_model_coarse_watermarks() {
    // Coarser cadence: fewer watermarks; still must be correct.
    run_range_window_reference_model(512).await;
}

