//! Smoke: basic WO/WRO behavior and representative custom aggregates.

use datafusion::scalar::ScalarValue;

use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::operators::operator::{OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::operator::WindowOperatorConfig;
use crate::test_utils::window::harness::{
    assert_window_values, batch, keyed_message, run_wo, watermark_message, window_exec_from_sql,
    Harness, WoWroHarness,
};
use crate::runtime::operators::window::{
    TileConfig, TimeGranularity, TASK_METADATA_ROWS_ACCEPTED, TASK_METADATA_ROWS_DROPPED_LATE,
};

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

    h.ingest(batch(vec![1500], vec![99.0], vec!["A"]), "A")
        .await;
    let out = h.watermark_and_output(3000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(out.num_rows(), 0);
    assert_eq!(
        h.task_metadata.get(TASK_METADATA_ROWS_ACCEPTED).as_deref(),
        Some("2")
    );
    assert_eq!(
        h.task_metadata
            .get(TASK_METADATA_ROWS_DROPPED_LATE)
            .as_deref(),
        Some("1")
    );
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
    assert_eq!(
        h.task_metadata.get(TASK_METADATA_ROWS_ACCEPTED).as_deref(),
        Some("1")
    );
    assert_eq!(
        h.task_metadata
            .get(TASK_METADATA_ROWS_DROPPED_LATE)
            .as_deref(),
        Some("0")
    );
}

#[tokio::test]
async fn top_and_categorical_aggregates_flow_through_wo_and_wro() {
    let cases = [
        (
            r#"SELECT timestamp, value, partition_key, top(value, 2) OVER w as top_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
)"#,
            vec![
                vec![ScalarValue::Utf8(Some("1".to_string()))],
                vec![ScalarValue::Utf8(Some("2,1".to_string()))],
                vec![ScalarValue::Utf8(Some("3,2".to_string()))],
            ],
            ScalarValue::Utf8(Some("3,2".to_string())),
            batch(
                vec![0, 300_000, 600_000],
                vec![1.0, 2.0, 3.0],
                vec!["K", "K", "K"],
            ),
        ),
        // Mixed A/B are cate groups; a constant PARTITION BY keeps them in one window
        // (PARTITION BY partition_key would isolate each key).
        (
            r#"SELECT timestamp, value, partition_key,
  sum_cate(value, partition_key) OVER w as sums
FROM test_table
WINDOW w AS (
  PARTITION BY 1
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
)"#,
            vec![
                vec![ScalarValue::Utf8(Some("A:1".to_string()))],
                vec![ScalarValue::Utf8(Some("A:1,B:2".to_string()))],
                vec![ScalarValue::Utf8(Some("A:4,B:2".to_string()))],
            ],
            ScalarValue::Utf8(Some("A:4,B:2".to_string())),
            batch(
                vec![0, 300_000, 600_000],
                vec![1.0, 2.0, 3.0],
                vec!["A", "B", "A"],
            ),
        ),
    ];

    for (sql, wo_expected, wro_expected, events) in cases {
        let tiling = TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap();
        let wo = run_wo(sql, Some(tiling.clone()), events.clone(), "K", 600_000).await;
        assert_window_values(&wo, 3, &wo_expected, "aggregate WO");

        let mut h = WoWroHarness::new(sql, Some(tiling), true).await;
        h.ingest(events, "K").await;
        h.advance(600_000).await;
        let out = h
            .request(batch(vec![600_000], vec![0.0], vec!["K"]), "K")
            .await;
        assert_window_values(&out, 3, &[vec![wro_expected]], "aggregate WRO");
    }
}

#[tokio::test]
async fn ingest_drains_ready_messages_then_stops_before_watermark() {
    let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let exec = window_exec_from_sql(sql).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;
    h.op.set_input(Some(Box::pin(futures::stream::iter(vec![
        keyed_message(batch(vec![1000], vec![1.0], vec!["A"]), "A"),
        keyed_message(batch(vec![2000], vec![2.0], vec!["A"]), "A"),
        watermark_message(2000),
    ]))));

    assert!(matches!(
        h.op.poll_next().await,
        OperatorPollResult::Continue
    ));
    let out = match h.op.poll_next().await {
        OperatorPollResult::Ready(Message::Regular(base)) => base.record_batch,
        other => panic!("expected emit on deferred watermark, got {other:?}"),
    };
    let wm = match h.op.poll_next().await {
        OperatorPollResult::Ready(Message::Watermark(w)) => w.watermark_value,
        other => panic!("expected passthrough watermark, got {other:?}"),
    };
    assert_eq!(wm, 2000);
    assert_window_values(
        &out,
        3,
        &[
            vec![ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(3.0))],
        ],
        "both data messages ingested before the watermark",
    );
}
