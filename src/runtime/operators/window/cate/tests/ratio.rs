use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};

#[tokio::test]
async fn test_top_n_key_ratio_cate_orders_by_key() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    top_n_key_ratio_cate(value, value > 1, partition_key, 2) OVER w as top_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 1.0, "a", 0),
        (2000, 2.0, "b", 1),
        (3000, 3.0, "b", 2),
        (4000, 1.0, "c", 3),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("c:0,b:1".to_string())));
}

#[tokio::test]
async fn test_top_n_value_ratio_cate_orders_by_value() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    top_n_value_ratio_cate(value, value > 1, partition_key, 2) OVER w as top_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 1.0, "a", 0),
        (2000, 2.0, "b", 1),
        (3000, 3.0, "b", 2),
        (4000, 1.0, "c", 3),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("b:1,c:0".to_string())));
}

#[tokio::test]
async fn test_top_n_ratio_all_match() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    top_n_value_ratio_cate(value, value > 0, partition_key, 1) OVER w as top_val
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
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("a:1".to_string())));
}
