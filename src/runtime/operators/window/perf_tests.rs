use std::env;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use datafusion::prelude::SessionContext;

use crate::api::planner::{Planner, PlanningContext};
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator, SEQ_NO_COLUMN_NAME};

fn parse_arg(name: &str, default: usize) -> usize {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            if let Some(v) = args.next() {
                if let Ok(parsed) = v.parse::<usize>() {
                    return parsed;
                }
            }
        }
    }
    default
}

fn test_schema_with_seq() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false),
    ]))
}

fn build_batches(rows: usize, batch_size: usize, categories: usize) -> Vec<RecordBatch> {
    let schema = test_schema_with_seq();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < rows {
        let end = (i + batch_size).min(rows);
        let len = end - i;
        let mut ts = Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);
        let mut cats = Vec::with_capacity(len);
        let mut seq = Vec::with_capacity(len);
        for row in i..end {
            ts.push((row as i64 + 1) * 1000);
            values.push((row % 100) as f64 / 100.0);
            cats.push(format!("c{}", row % categories.max(1)));
            seq.push(row as u64);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(cats)),
                Arc::new(UInt64Array::from(seq)),
            ],
        )
        .expect("batch");
        out.push(batch);
        i = end;
    }
    out
}

async fn window_exprs_from_sql(sql: &str) -> Vec<Arc<dyn WindowExpr>> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        test_schema_with_seq(),
    );
    let exec = extract_datafusion_window_exec(sql, &mut planner).await;
    exec.window_expr().to_vec()
}

async fn run_case(name: String, sql: &str, batches: &[RecordBatch], iters: usize) {
    let window_exprs = window_exprs_from_sql(sql).await;
    let mut accs = Vec::with_capacity(window_exprs.len());
    for expr in &window_exprs {
        let acc = match create_window_aggregator(expr) {
            WindowAggregator::Accumulator(acc) => acc,
            _ => panic!("expected accumulator"),
        };
        accs.push((expr.clone(), acc));
    }

    let start = Instant::now();
    for _ in 0..iters {
        for batch in batches {
            for (expr, acc) in accs.iter_mut() {
                let args = expr.evaluate_args(batch).expect("eval args");
                acc.update_batch(&args).expect("update");
            }
        }
        for (_, acc) in accs.iter_mut() {
            let _ = acc.evaluate().expect("evaluate");
        }
    }
    let elapsed = start.elapsed();
    let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() * iters;
    let rows_per_sec = (total_rows as f64) / elapsed.as_secs_f64();
    println!(
        "{name}: {total_rows} rows in {:.3}s ({:.0} rows/s)",
        elapsed.as_secs_f64(),
        rows_per_sec
    );
}

#[tokio::test]
#[ignore]
async fn perf_custom_aggs() {
    let rows = parse_arg("--rows", 200_000);
    let batch_size = parse_arg("--batch", 4096);
    let categories = parse_arg("--cate", 100);
    let iters = parse_arg("--iters", 3);

    let batches = build_batches(rows, batch_size, categories);
    let window = "WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let agg_kinds = ["sum", "count", "avg", "min", "max"];
    let cate_variants = [
        ("regular", "{}(value) OVER w as v0"),
        ("where", "{}_where(value, value > 0.5) OVER w as v0"),
        ("cate", "{}_cate(value, partition_key) OVER w as v0"),
        ("cate_where", "{}_cate_where(value, value > 0.5, partition_key) OVER w as v0"),
    ];

    for agg in agg_kinds {
        for (label, template) in cate_variants {
            let select = template.replace("{}", agg);
            let sql = format!(
                "SELECT timestamp, value, partition_key, {select} FROM test_table {window}"
            );
            run_case(format!("{agg}/{label}"), &sql, &batches, iters).await;
        }
    }

    let top_variants = [
        ("top", "top(value, 5) OVER w as v0"),
        ("topn_frequency", "topn_frequency(value, 5) OVER w as v0"),
        ("top1_ratio", "top1_ratio(value) OVER w as v0"),
    ];
    for (label, select) in top_variants {
        let sql = format!(
            "SELECT timestamp, value, partition_key, {select} FROM test_table {window}"
        );
        run_case(format!("top/{label}"), &sql, &batches, iters).await;
    }

    for agg in agg_kinds {
        let key_sql = format!(
            "SELECT timestamp, value, partition_key, \
            top_n_key_{agg}_cate_where(value, value > 0.5, partition_key, 5) OVER w as v0 \
            FROM test_table {window}"
        );
        run_case(format!("top_n_key_{agg}"), &key_sql, &batches, iters).await;

        let value_sql = format!(
            "SELECT timestamp, value, partition_key, \
            top_n_value_{agg}_cate_where(value, value > 0.5, partition_key, 5) OVER w as v0 \
            FROM test_table {window}"
        );
        run_case(format!("top_n_value_{agg}"), &value_sql, &batches, iters).await;
    }

    let ratio_variants = [
        (
            "top_n_key_ratio_cate",
            "top_n_key_ratio_cate(value, value > 0.5, partition_key, 5) OVER w as v0",
        ),
        (
            "top_n_value_ratio_cate",
            "top_n_value_ratio_cate(value, value > 0.5, partition_key, 5) OVER w as v0",
        ),
    ];
    for (label, select) in ratio_variants {
        let sql = format!(
            "SELECT timestamp, value, partition_key, {select} FROM test_table {window}"
        );
        run_case(label.to_string(), &sql, &batches, iters).await;
    }
}
