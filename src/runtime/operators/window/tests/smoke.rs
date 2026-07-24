//! Smoke: basic WO / WRO behavior (not tiling equivalence).

use std::sync::Arc;

use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::store::StateNamespace;
use crate::runtime::operators::window::tests::harness::{
    batch, col_f64, keyed_message, runtime_context, watermark_message, window_exec_from_sql,
    Harness,
};
use crate::runtime::operators::window::window_operator::{
    WindowEmitMode, WindowOperator, WindowOperatorConfig,
};
use crate::runtime::operators::window::window_request_operator::{
    WindowRequestOperator, WindowRequestOperatorConfig,
};
use crate::storage::{InMemSortedKV, SortedKV};

#[tokio::test]
async fn range_sum_basic() {
    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;

    h.ingest(
        batch(
            vec![1000, 2000, 3000],
            vec![10.0, 20.0, 30.0],
            vec!["A", "A", "A"],
        ),
        "A",
    )
    .await;
    let out = h.watermark_and_output(3000).await;
    let _ = h.drain_passthrough_watermark().await;

    assert_eq!(out.num_rows(), 3);
    let sum_col = out.num_columns() - 1;
    assert!((col_f64(&out, sum_col).value(0) - 10.0).abs() < 1e-9);
    assert!((col_f64(&out, sum_col).value(1) - 30.0).abs() < 1e-9);
    assert!((col_f64(&out, sum_col).value(2) - 60.0).abs() < 1e-9);
}

#[tokio::test]
async fn drop_post_frontier_late_events() {
    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;

    h.ingest(batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;
    let _ = h.watermark_and_output(2000).await;
    let _ = h.drain_passthrough_watermark().await;

    h.ingest(batch(vec![1500], vec![99.0], vec!["A"]), "A").await;
    let out = h.watermark_and_output(3000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(out.num_rows(), 0);
}

#[tokio::test]
async fn terminal_max_watermark() {
    let sql = r#"SELECT timestamp, value, partition_key, COUNT(value) OVER w as cnt
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;

    h.ingest(batch(vec![1000], vec![1.0], vec!["A"]), "A").await;
    let out = h.watermark_and_output(MAX_WATERMARK_VALUE).await;
    let wm = h.drain_passthrough_watermark().await;
    assert_eq!(wm, MAX_WATERMARK_VALUE);
    assert_eq!(out.num_rows(), 1);
}

#[tokio::test]
async fn two_clients_wo_wro_share_backend() {
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
