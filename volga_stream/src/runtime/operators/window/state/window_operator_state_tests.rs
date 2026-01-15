//! State/storage-focused tests for the window operator implementation.
//!
//! These tests intentionally exercise:
//! - dumping to the store (persisted layout)
//! - read-compaction / rehydration paths
//! - pruning invariants
//!
//! They live separately from `window_operator_tests.rs` to keep operator semantics tests clean.

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
use crate::runtime::operators::window::window_operator::{WindowOperator, WindowOperatorConfig};
use crate::runtime::operators::window::shared::build_window_operator_parts;
use crate::runtime::operators::window::TimeGranularity;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::OperatorStates;
use crate::storage::batch_store::{BatchStore, InMemBatchStore};
use crate::storage::{StorageBudgetConfig, WorkerStorageContext};
use crate::common::Key;

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

fn col_i64(b: &RecordBatch, idx: usize) -> &Int64Array {
    b.column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 col")
}

struct Harness {
    op: WindowOperator,
}

impl Harness {
    async fn new(cfg: WindowOperatorConfig) -> Self {
        let ctx = runtime_context();
        let mut op = WindowOperator::new(OperatorConfig::WindowConfig(cfg));
        op.open(&ctx).await.expect("open");
        Self { op }
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

    async fn watermark_output(&mut self, wm: u64) -> RecordBatch {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            watermark_message(wm),
        ]))));
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Regular(base)) => base.record_batch,
            other => panic!("expected regular output batch, got {:?}", other),
        }
    }

    async fn drain_passthrough_watermark(&mut self) -> u64 {
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Watermark(wm)) => wm.watermark_value,
            other => panic!("expected passthrough watermark, got {:?}", other),
        }
    }

    fn state(&self) -> &crate::runtime::operators::window::window_operator_state::WindowOperatorState {
        self.op.get_state()
    }

    async fn close(mut self) {
        self.op.close().await.expect("close");
    }
}

#[tokio::test]
async fn test_pruning_with_lateness_mixed_windows_and_mixed_aggs() {
    // Same scenario as the legacy pruning test, kept here because it validates state invariants.
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
    cfg.parallelize = true;
    cfg.lateness = Some(3000);
    let mut h = Harness::new(cfg).await;

    let partition_key = key("A");

    h.ingest(
        batch(vec![10000, 15000, 20000], vec![100.0, 150.0, 200.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out1 = h.watermark_output(20000).await;
    assert_eq!(out1.num_rows(), 2);
    h.state()
        .verify_pruning_for_testing(&partition_key, 0)
        .await;
    assert_eq!(h.drain_passthrough_watermark().await, 20000);

    h.ingest(
        batch(vec![16000, 18000, 25000], vec![160.0, 180.0, 250.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out2 = h.watermark_output(25000).await;
    // With per-key lateness on ingest, 18000 is kept (cutoff=17000),
    // so watermark=25000 (advance_to=22000) emits [18000, 20000].
    assert_eq!(out2.num_rows(), 2);
    h.state()
        .verify_pruning_for_testing(&partition_key, 10000)
        .await;
    assert_eq!(h.drain_passthrough_watermark().await, 25000);

    h.ingest(
        batch(
            vec![26000, 27000, 28000, 29000, 30000],
            vec![260.0, 270.0, 280.0, 290.0, 300.0],
            vec!["A", "A", "A", "A", "A"],
        ),
        "A",
    )
    .await;
    let out3 = h.watermark_output(30000).await;
    assert_eq!(out3.num_rows(), 3);
    h.state()
        .verify_pruning_for_testing(&partition_key, 10000)
        .await;
    assert_eq!(h.drain_passthrough_watermark().await, 30000);

    let _ = h.watermark_output(MAX_WATERMARK_VALUE).await;
    let _ = h.drain_passthrough_watermark().await;
    h.close().await;
}

#[tokio::test]
async fn test_compaction_persisted_plus_new_deltas_does_not_drop_history() {
    // Regression test for: persisted bucket + new hot deltas → compaction must include persisted history.
    let sql = "SELECT
        timestamp, value, partition_key,
        COUNT(value) OVER w as cnt
      FROM test_table
      WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp
        RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let window_exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(window_exec);
    cfg.parallelize = false;
    cfg.dump_hot_bucket_count = 0; // force persisted-only buckets
    let mut h = Harness::new(cfg).await;

    let key_a = key("A");
    let bucket_ts: i64 = 1000;

    let ts1: Vec<i64> = (1000..1100).collect();
    h.ingest(batch(ts1, vec![1.0; 100], vec!["A"; 100]), "A")
        .await;
    let out1 = h.watermark_output(1099).await;
    assert_eq!(out1.num_rows(), 100);
    assert_eq!(h.drain_passthrough_watermark().await, 1099);

    h.state().periodic_dump_to_store().await.expect("dump");

    let ts2: Vec<i64> = (1100..1110).collect();
    h.ingest(batch(ts2, vec![1.0; 10], vec!["A"; 10]), "A")
        .await;
    h.state().compact_bucket_on_read(&key_a, bucket_ts).await;

    let out2 = h.watermark_output(1109).await;
    assert_eq!(out2.num_rows(), 10);
    let cnt = col_i64(&out2, 3);
    assert_eq!(cnt.value(out2.num_rows() - 1), 110);
    assert_eq!(h.drain_passthrough_watermark().await, 1109);

    h.close().await;
}

#[tokio::test]
async fn test_ingest_triggers_pressure_relief_when_in_mem_over_limit() {
    // Moved from `window_operator_state.rs` (state-level behavior).
    use crate::storage::StorageStats;

    let stats_before = StorageStats::global().snapshot();

    let budgets = StorageBudgetConfig {
        in_mem_limit_bytes: 1024,
        in_mem_low_watermark_per_mille: 500,
        work_limit_bytes: 1024 * 1024,
        max_inflight_keys: 1,
        load_io_parallelism: 1,
    };

    let store: Arc<dyn BatchStore> = Arc::new(InMemBatchStore::new(
        4,
        TimeGranularity::Seconds(1),
        256,
    ));
    let storage = WorkerStorageContext::new(store, budgets.clone()).unwrap();

    let sql = "SELECT
        timestamp, value,
        COUNT(value) OVER w as cnt
      FROM test_table
      WINDOW w AS (PARTITION BY value ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
    let window_exec = window_exec_from_sql(sql).await;
    let (_ts_idx, windows, _in_schema, _out_schema, _tp) =
        build_window_operator_parts(false, &window_exec, &Vec::new(), false);
    let state = crate::runtime::operators::window::window_operator_state::WindowOperatorState::new(
        storage,
        Arc::<str>::from("t"),
        0, // timestamp column index
        Arc::new(windows),
        Vec::new(),
        None,
        0, // dump_hot_bucket_count: treat everything as cold for eviction
        budgets.in_mem_low_watermark_per_mille,
        4, // in_mem_dump_parallelism
    );

    let k = key("A");
    let ts: Vec<i64> = (0..4096).collect();
    let b = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(vec![1.0; 4096])),
        ],
    )
    .unwrap();

    let _ = state.insert_batch(&k, None, b).await;

    let limit = state.in_mem_batch_cache().limit_bytes();
    let low = (limit.saturating_mul(budgets.in_mem_low_watermark_per_mille as usize)) / 1000;
    assert!(
        state.in_mem_batch_cache().bytes() <= low,
        "expected eviction down to low watermark (<= {}), got {}",
        low,
        state.in_mem_batch_cache().bytes()
    );

    let stats_after = StorageStats::global().snapshot();
    assert!(
        stats_after.pressure_relief_runs > stats_before.pressure_relief_runs,
        "expected at least one pressure relief run"
    );
    assert!(
        stats_after.pressure_dumped_buckets > stats_before.pressure_dumped_buckets,
        "expected at least one bucket dumped during pressure relief"
    );
}

