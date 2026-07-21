//! RANGE-only WindowOperator tests (SortedKV).

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::prelude::SessionContext;

use crate::api::planner::{Planner, PlanningContext};
use crate::common::message::Message;
use crate::common::{Key, WatermarkMessage, MAX_WATERMARK_VALUE};
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::store::StateNamespace;
use crate::runtime::operators::window::window_operator::{
    WindowEmitMode, WindowOperator, WindowOperatorConfig,
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::OperatorStates;
use crate::storage::{InMemSortedKV, SortedKV};

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
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(vals)),
            Arc::new(StringArray::from(keys)),
        ],
    )
    .expect("test batch")
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
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        test_input_schema(),
    );
    extract_datafusion_window_exec(sql, &mut planner).await
}

fn runtime_context(kv: Arc<dyn SortedKV>) -> RuntimeContext {
    let mut ctx = RuntimeContext::new(
        "test_vertex".to_string().into(),
        0,
        1,
        None,
        Some(Arc::new(OperatorStates::new())),
        None,
    );
    ctx.set_sorted_kv(kv);
    ctx.set_window_state_namespace(StateNamespace::new(b"window_state"));
    ctx
}

struct Harness {
    op: WindowOperator,
}

impl Harness {
    async fn new(cfg: WindowOperatorConfig) -> Self {
        let kv: Arc<dyn SortedKV> = Arc::new(InMemSortedKV::new());
        let ctx = runtime_context(kv);
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

    async fn watermark_and_output(&mut self, wm: u64) -> RecordBatch {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            watermark_message(wm),
        ]))));
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Regular(base)) => base.record_batch,
            other => panic!("expected output on watermark, got {:?}", other),
        }
    }

    async fn drain_passthrough_watermark(&mut self) -> u64 {
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Watermark(wm)) => wm.watermark_value,
            other => panic!("expected passthrough watermark, got {:?}", other),
        }
    }
}

fn col_f64(batch: &RecordBatch, idx: usize) -> &Float64Array {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64")
}

#[tokio::test]
async fn test_range_sum_basic() {
    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let cfg = WindowOperatorConfig::new(exec);
    let mut h = Harness::new(cfg).await;

    h.ingest(
        batch(vec![1000, 2000, 3000], vec![10.0, 20.0, 30.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    let out = h.watermark_and_output(3000).await;
    let _ = h.drain_passthrough_watermark().await;

    assert_eq!(out.num_rows(), 3);
    // window cols after input: sum at last column
    let sum_col = out.num_columns() - 1;
    assert!((col_f64(&out, sum_col).value(0) - 10.0).abs() < 1e-9);
    assert!((col_f64(&out, sum_col).value(1) - 30.0).abs() < 1e-9);
    assert!((col_f64(&out, sum_col).value(2) - 60.0).abs() < 1e-9);
}

#[tokio::test]
async fn test_drop_post_frontier_late_events() {
    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    // Retention lateness is irrelevant: post-frontier ingest is dropped on processed_pos.
    let cfg = WindowOperatorConfig::new(exec);
    let mut h = Harness::new(cfg).await;

    h.ingest(batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;
    let _ = h.watermark_and_output(2000).await;
    let _ = h.drain_passthrough_watermark().await;

    // Streaming late: ts <= processed_pos after advance to 2000
    h.ingest(batch(vec![1500], vec![99.0], vec!["A"]), "A").await;
    let out = h.watermark_and_output(3000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(out.num_rows(), 0);
}

#[tokio::test]
async fn test_terminal_max_watermark() {
    let sql = r#"SELECT timestamp, value, partition_key, COUNT(value) OVER w as cnt
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let cfg = WindowOperatorConfig::new(exec);
    let mut h = Harness::new(cfg).await;

    h.ingest(batch(vec![1000], vec![1.0], vec!["A"]), "A").await;
    let out = h.watermark_and_output(MAX_WATERMARK_VALUE).await;
    let wm = h.drain_passthrough_watermark().await;
    assert_eq!(wm, MAX_WATERMARK_VALUE);
    assert_eq!(out.num_rows(), 1);
}

#[tokio::test]
async fn test_two_clients_wo_wro_share_backend() {
    use crate::runtime::operators::window::window_request_operator::{
        WindowRequestOperator, WindowRequestOperatorConfig,
    };

    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let kv = Arc::new(InMemSortedKV::new());
    let ns = StateNamespace::new(b"window_state");

    let mut wo_ctx = runtime_context(kv.clone() as Arc<dyn SortedKV>);
    wo_ctx.set_window_state_namespace(ns.clone());
    let mut wo_cfg = WindowOperatorConfig::new(exec.clone());
    wo_cfg.execution_mode = WindowEmitMode::Request;
    let mut wo = WindowOperator::new(OperatorConfig::WindowConfig(wo_cfg));
    wo.open(&wo_ctx).await.expect("wo open");

    let mut wro_ctx = runtime_context(kv as Arc<dyn SortedKV>);
    wro_ctx.set_window_state_namespace(ns);
    let req_cfg = WindowRequestOperatorConfig::from_window_operator_config(
        WindowOperatorConfig::new(exec),
    );
    let mut wro = WindowRequestOperator::new(OperatorConfig::WindowRequestConfig(req_cfg));
    wro.open(&wro_ctx).await.expect("wro open");

    wo.set_input(Some(Box::pin(futures::stream::iter(vec![keyed_message(
        batch(vec![1000, 2000], vec![10.0, 20.0], vec!["A", "A"]),
        "A",
    )]))));
    assert!(matches!(wo.poll_next().await, OperatorPollResult::Continue));

    wo.set_input(Some(Box::pin(futures::stream::iter(vec![watermark_message(
        2000,
    )]))));
    // Request mode: continue then passthrough watermark
    assert!(matches!(wo.poll_next().await, OperatorPollResult::Continue));
    assert!(matches!(
        wo.poll_next().await,
        OperatorPollResult::Ready(Message::Watermark(_))
    ));

    wro.set_input(Some(Box::pin(futures::stream::iter(vec![keyed_message(
        batch(vec![2000], vec![0.0], vec!["A"]),
        "A",
    )]))));
    match wro.poll_next().await {
        OperatorPollResult::Ready(Message::Regular(base)) => {
            assert_eq!(base.record_batch.num_rows(), 1);
        }
        other => panic!("expected WRO result, got {:?}", other),
    }
}
