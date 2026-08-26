//! WO ingest uses `next_inputs` so one poll can take several ready data messages.

use crate::common::message::Message;
use crate::runtime::operators::operator::{OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::operator::WindowOperatorConfig;
use crate::test_utils::window::harness::{
    assert_window_values, batch, keyed_message, watermark_message, window_exec_from_sql, Harness,
};
use datafusion::scalar::ScalarValue;

const SQL: &str = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;

#[tokio::test]
async fn ingest_drains_ready_keys_then_stops_before_watermark() {
    let exec = window_exec_from_sql(SQL).await;
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
