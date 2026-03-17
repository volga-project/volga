use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};

fn expected(agg: &str) -> &'static str {
    match agg {
        "sum" => "c:6,b:8",
        "count" => "c:2,b:2",
        "avg" => "c:3,b:4",
        "min" => "c:2,b:3",
        "max" => "c:4,b:5",
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_top_n_key_cate_where_by_agg() {
    let aggs = ["sum", "count", "avg", "min", "max"];
    for agg in aggs {
        let sql = format!(
            "SELECT\n    timestamp,\n    value,\n    partition_key,\n    top_n_key_{agg}_cate_where(value, value > 0, partition_key, 2) OVER w as top_val\nFROM test_table\nWINDOW w AS (\n  PARTITION BY partition_key\n  ORDER BY timestamp\n  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW\n)"
        );
        let window_expr = test_utils::window_expr_from_sql(&sql).await;
        let batch = test_utils::batch(&[
            (1000, 1.0, "a", 0),
            (2000, 3.0, "b", 1),
            (3000, 5.0, "b", 2),
            (4000, 2.0, "c", 3),
            (5000, 4.0, "c", 4),
        ]);

        let mut acc = match create_window_aggregator(&window_expr) {
            WindowAggregator::Accumulator(acc) => acc,
            _ => panic!("expected accumulator"),
        };
        let args = window_expr.evaluate_args(&batch).expect("eval args");
        acc.update_batch(&args).expect("update");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(out, ScalarValue::Utf8(Some(expected(agg).to_string())));
    }
}
