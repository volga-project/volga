use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};

#[tokio::test]
async fn test_topn_frequency() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    topn_frequency(value, 2) OVER w as top_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 1.0, "a", 0),
        (2000, 1.0, "a", 1),
        (3000, 2.0, "a", 2),
        (4000, 2.0, "a", 3),
        (5000, 2.0, "a", 4),
        (6000, 3.0, "a", 5),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("2,1".to_string())));
}

#[tokio::test]
async fn test_topn_frequency_ties() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    topn_frequency(value, 3) OVER w as top_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 1.0, "a", 0),
        (2000, 2.0, "a", 1),
        (3000, 3.0, "a", 2),
        (4000, 1.0, "a", 3),
        (5000, 2.0, "a", 4),
        (6000, 3.0, "a", 5),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("3,2,1".to_string())));
}
