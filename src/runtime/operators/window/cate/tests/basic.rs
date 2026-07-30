use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggs::test_utils;
use crate::runtime::operators::window::create_window_accumulator;

#[tokio::test]
async fn test_sum_cate_where_retracts() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    sum_cate_where(value, value > 2, partition_key) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 5.0, "a", 0),
        (2000, 2.0, "b", 1),
        (3000, 1.0, "a", 2),
    ]);
    let retract_batch = test_utils::batch(&[(1000, 5.0, "a", 0)]);

    let mut acc = create_window_accumulator(&window_expr);
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("a:5".to_string())));

    let retract_args = window_expr
        .evaluate_args(&retract_batch)
        .expect("eval retract args");
    acc.retract_batch(&retract_args).expect("retract");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some(String::new())));
}
