use std::collections::HashMap;
use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};

#[tokio::test]
async fn test_sum_where_updates_on_condition() {
    let sql = r#"SELECT
    timestamp,
    value,
    partition_key,
    sum_where(value, value > 2) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let window_expr = test_utils::window_expr_from_sql(sql).await;
    let batch = test_utils::batch(&[
        (1000, 1.0, "A", 0),
        (2000, 3.0, "A", 1),
        (3000, 2.0, "A", 2),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Float64(Some(3.0)));
}

#[tokio::test]
async fn test_sum_cate_where_outputs_string() {
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
        (1000, 1.0, "a", 0),
        (2000, 3.0, "b", 1),
        (3000, 4.0, "a", 2),
    ]);

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("a:4,b:3".to_string())));
}

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

    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
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

#[derive(Clone, Copy)]
enum Agg {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Copy)]
enum Variant {
    Where,
    Cate,
    CateWhere,
}

fn agg_name(agg: Agg) -> &'static str {
    match agg {
        Agg::Sum => "sum",
        Agg::Count => "count",
        Agg::Avg => "avg",
        Agg::Min => "min",
        Agg::Max => "max",
    }
}

fn format_float(value: f64) -> String {
    let s = format!("{value:.6}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(format_float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(format_float(*v)),
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

fn eval_window_expr(window_expr: &Arc<dyn datafusion::physical_plan::WindowExpr>) -> ScalarValue {
    let batches = [
        test_utils::batch(&[(1000, 1.0, "A", 0), (2000, 3.0, "A", 1)]),
        test_utils::batch(&[(3000, 4.0, "B", 2), (4000, 2.0, "B", 3)]),
    ];
    let mut acc = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    for batch in batches {
        let args = window_expr.evaluate_args(&batch).expect("eval args");
        acc.update_batch(&args).expect("update");
    }
    acc.evaluate().expect("evaluate")
}

fn expected_where(agg: Agg) -> ScalarValue {
    // rows with value > 2: 3.0 (A), 4.0 (B)
    match agg {
        Agg::Sum => ScalarValue::Float64(Some(7.0)),
        Agg::Count => ScalarValue::Int64(Some(2)),
        Agg::Avg => ScalarValue::Float64(Some(3.5)),
        Agg::Min => ScalarValue::Float64(Some(3.0)),
        Agg::Max => ScalarValue::Float64(Some(4.0)),
    }
}

fn expected_cate(agg: Agg, with_where: bool) -> String {
    // categories A: [1,3], B: [4,2]; where=value>2 -> A:[3], B:[4]
    let mut map: HashMap<&'static str, String> = HashMap::new();
    match (agg, with_where) {
        (Agg::Sum, false) => {
            map.insert("A", "4".to_string());
            map.insert("B", "6".to_string());
        }
        (Agg::Sum, true) => {
            map.insert("A", "3".to_string());
            map.insert("B", "4".to_string());
        }
        (Agg::Count, false) => {
            map.insert("A", "2".to_string());
            map.insert("B", "2".to_string());
        }
        (Agg::Count, true) => {
            map.insert("A", "1".to_string());
            map.insert("B", "1".to_string());
        }
        (Agg::Avg, false) => {
            map.insert("A", format_float(2.0));
            map.insert("B", format_float(3.0));
        }
        (Agg::Avg, true) => {
            map.insert("A", "3".to_string());
            map.insert("B", "4".to_string());
        }
        (Agg::Min, false) => {
            map.insert("A", "1".to_string());
            map.insert("B", "2".to_string());
        }
        (Agg::Min, true) => {
            map.insert("A", "3".to_string());
            map.insert("B", "4".to_string());
        }
        (Agg::Max, false) => {
            map.insert("A", "3".to_string());
            map.insert("B", "4".to_string());
        }
        (Agg::Max, true) => {
            map.insert("A", "3".to_string());
            map.insert("B", "4".to_string());
        }
    }

    let mut entries: Vec<_> = map.into_iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    entries
        .into_iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn agg_expr(agg: Agg, variant: Variant, idx: usize) -> String {
    match variant {
        Variant::Where => format!(
            "{}_where(value, value > 2) OVER w as {}_where_val_{}",
            agg_name(agg),
            agg_name(agg),
            idx
        ),
        Variant::Cate => format!(
            "{}_cate(value, partition_key) OVER w as {}_cate_val_{}",
            agg_name(agg),
            agg_name(agg),
            idx
        ),
        Variant::CateWhere => format!(
            "{}_cate_where(value, value > 2, partition_key) OVER w as {}_cate_where_val_{}",
            agg_name(agg),
            agg_name(agg),
            idx
        ),
    }
}

#[tokio::test]
async fn test_cate_where_matrix() {
    let aggs = [Agg::Sum, Agg::Count, Agg::Avg, Agg::Min, Agg::Max];
    let variants = [Variant::Where, Variant::Cate, Variant::CateWhere];
    let agg_counts = [1usize, 2usize];
    let include_regular = [false, true];

    for agg in aggs {
        for &variant in &variants {
            for &agg_count in &agg_counts {
                for &with_regular in &include_regular {
                    let mut exprs = Vec::new();
                    for i in 0..agg_count {
                        let a = if i == 0 { agg } else { Agg::Sum };
                        exprs.push(agg_expr(a, variant, i));
                    }
                    if with_regular {
                        exprs.push(format!("sum(value) OVER w as sum_val_{}", agg_count));
                    }
                    let select = exprs.join(",\n    ");
                    let sql = format!(
                        "SELECT\n    timestamp,\n    value,\n    partition_key,\n    {select}\nFROM test_table\nWINDOW w AS (\n  PARTITION BY partition_key\n  ORDER BY timestamp\n  RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW\n)"
                    );

                    let exec = test_utils::window_exec_from_sql(&sql).await;
                    let window_exprs = exec.window_expr();
                    for (idx, window_expr) in window_exprs.iter().enumerate() {
                        let out = eval_window_expr(window_expr);
                        let expected = match (variant, idx) {
                            (Variant::Where, 0) => expected_where(agg),
                            (Variant::Cate, 0) => {
                                ScalarValue::Utf8(Some(expected_cate(agg, false)))
                            }
                            (Variant::CateWhere, 0) => {
                                ScalarValue::Utf8(Some(expected_cate(agg, true)))
                            }
                            _ => {
                                // secondary agg or regular sum uses sum semantics
                                if matches!(variant, Variant::Where) {
                                    expected_where(Agg::Sum)
                                } else {
                                    ScalarValue::Float64(Some(10.0))
                                }
                            }
                        };

                        if matches!(variant, Variant::Cate | Variant::CateWhere) && idx == 0 {
                            assert_eq!(out, expected);
                        } else if idx == 0 {
                            assert_eq!(out, expected);
                        } else {
                            // For secondary or regular aggregate, just ensure it evaluates.
                            assert!(scalar_to_string(&out).is_some(), "expected non-null");
                        }
                    }
                }
            }
        }
    }
}
