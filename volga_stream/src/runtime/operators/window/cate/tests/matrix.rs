use std::collections::HashMap;
use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use super::eval_window_expr;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
use datafusion::physical_plan::WindowExpr;

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

fn expected_for_name(name: &str) -> ScalarValue {
    let (kind, suffix) = if let Some(stripped) = name.strip_suffix("_cate_where") {
        (stripped, "cate_where")
    } else if let Some(stripped) = name.strip_suffix("_cate") {
        (stripped, "cate")
    } else if let Some(stripped) = name.strip_suffix("_where") {
        (stripped, "where")
    } else {
        (name, "regular")
    };

    let agg = match kind {
        "sum" => Agg::Sum,
        "count" => Agg::Count,
        "avg" => Agg::Avg,
        "min" => Agg::Min,
        "max" => Agg::Max,
        _ => return ScalarValue::Float64(Some(10.0)),
    };

    match suffix {
        "where" => expected_where(agg),
        "cate" => ScalarValue::Utf8(Some(expected_cate(agg, false))),
        "cate_where" => ScalarValue::Utf8(Some(expected_cate(agg, true))),
        "regular" => ScalarValue::Float64(Some(10.0)),
        _ => ScalarValue::Float64(Some(10.0)),
    }
}

fn agg_name_from_window_expr(window_expr: &Arc<dyn WindowExpr>) -> &str {
    if let Some(plain_expr) = window_expr.as_any().downcast_ref::<PlainAggregateWindowExpr>() {
        return plain_expr.get_aggregate_expr().fun().name();
    }
    if let Some(sliding_expr) = window_expr
        .as_any()
        .downcast_ref::<SlidingAggregateWindowExpr>()
    {
        return sliding_expr.get_aggregate_expr().fun().name();
    }
    panic!("window expr is not aggregate");
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
                        let _ = idx;
                        let _ = variant;
                        let _ = agg;
                        let name = agg_name_from_window_expr(window_expr);
                        let expected = expected_for_name(name);
                        assert_eq!(out, expected);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn test_cate_where_matrix_with_retract() {
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
    let batch = test_utils::batch(&[(1000, 1.0, "A", 0), (2000, 4.0, "A", 1)]);
    let retract_batch = test_utils::batch(&[(2000, 4.0, "A", 1)]);
    let mut acc = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    let args = window_expr.evaluate_args(&batch).expect("eval args");
    acc.update_batch(&args).expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("A:4".to_string())));

    let retract_args = window_expr
        .evaluate_args(&retract_batch)
        .expect("eval retract args");
    acc.retract_batch(&retract_args).expect("retract");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some(String::new())));
}
