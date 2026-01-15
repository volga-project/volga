use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;

use crate::runtime::tests::watermark_streaming_e2e_test::run_watermark_window_pipeline;

#[derive(Debug, Clone)]
struct BenchCase {
    name: String,
    parallelism: usize,
    out_of_orderness_ms: u64,
    lateness_ms: i64,
    step_ms: i64,
    rate: Option<f32>,
}

#[derive(Debug)]
struct BenchRunResult {
    case_name: String,
    run_idx: usize,
    produced: usize,
    expected: usize,
    missed: usize,
    parallelism: usize,
    out_of_orderness_ms: u64,
    lateness_ms: i64,
    step_ms: i64,
    rate: Option<f32>,
}

#[derive(Debug)]
struct BenchSummary {
    case_name: String,
    min_missed: usize,
    max_missed: usize,
    avg_missed: usize,
    avg_drop_rate: f64,
}

fn bench_output_path() -> PathBuf {
    let out_dir = std::env::var("WM_BENCH_OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("tmp"));
    let _ = fs::create_dir_all(&out_dir);
    out_dir.join("watermark_streaming_benchmark.json")
}

fn write_bench_json(
    path: &PathBuf,
    total_records: usize,
    runs: usize,
    results: &[BenchRunResult],
    summary: &[BenchSummary],
) -> Result<()> {
    let mut buf = String::new();
    buf.push_str("{\n");
    buf.push_str(&format!("  \"total_records\": {},\n", total_records));
    buf.push_str(&format!("  \"runs\": {},\n", runs));
    buf.push_str("  \"results\": [\n");
    for (i, r) in results.iter().enumerate() {
        let rate_str = r
            .rate
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "null".to_string());
        buf.push_str(&format!(
            "    {{\"case\":\"{}\",\"run\":{},\"produced\":{},\"expected\":{},\"missed\":{},\"parallelism\":{},\"ooo_ms\":{},\"lateness_ms\":{},\"step_ms\":{},\"rate\":{}}}",
            r.case_name,
            r.run_idx,
            r.produced,
            r.expected,
            r.missed,
            r.parallelism,
            r.out_of_orderness_ms,
            r.lateness_ms,
            r.step_ms,
            rate_str
        ));
        if i + 1 != results.len() {
            buf.push_str(",\n");
        } else {
            buf.push('\n');
        }
    }
    buf.push_str("  ],\n");
    buf.push_str("  \"summary\": [\n");
    for (i, s) in summary.iter().enumerate() {
        buf.push_str(&format!(
            "    {{\"case\":\"{}\",\"min_missed\":{},\"max_missed\":{},\"avg_missed\":{},\"avg_drop_rate\":{:.6}}}",
            s.case_name, s.min_missed, s.max_missed, s.avg_missed, s.avg_drop_rate
        ));
        if i + 1 != summary.len() {
            buf.push_str(",\n");
        } else {
            buf.push('\n');
        }
    }
    buf.push_str("  ]\n");
    buf.push_str("}\n");
    fs::write(path, buf)?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_watermark_streaming_parallel_disorder_benchmark_matrix() -> Result<()> {
    let total_records: usize = std::env::var("WM_BENCH_TOTAL_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12_000);
    let runs: usize = std::env::var("WM_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    let parallelism_vals = vec![1usize, 4];
    let ooo_late_pairs: Vec<(u64, i64)> = vec![(0, 0), (50, 50), (250, 250)];
    let rate_step_pairs: Vec<(Option<f32>, i64)> =
        vec![(None, 1), (Some(6000.0), 1)];

    let mut cases: Vec<BenchCase> = Vec::new();
    for p in parallelism_vals {
        for (ooo, late) in &ooo_late_pairs {
            for (rate, step) in &rate_step_pairs {
                let rate_tag = rate.map(|r| format!("{:.0}", r)).unwrap_or_else(|| "max".to_string());
                let name = format!(
                    "p{}_ooo{}_lat{}_rate{}_step{}",
                    p, ooo, late, rate_tag, step
                );
                cases.push(BenchCase {
                    name,
                    parallelism: p,
                    out_of_orderness_ms: *ooo,
                    lateness_ms: *late,
                    step_ms: *step,
                    rate: *rate,
                });
            }
        }
    }

    let mut results: Vec<BenchRunResult> = Vec::new();
    let mut summary: Vec<BenchSummary> = Vec::new();

    for case in cases {
        let mut misses: Vec<usize> = Vec::with_capacity(runs);
        for run_idx in 0..runs {
            let rows = run_watermark_window_pipeline(
                case.parallelism,
                case.parallelism, // keys per task
                total_records,
                case.out_of_orderness_ms,
                case.lateness_ms,
                case.step_ms,
                case.rate,
            )
            .await?;

            // No duplicates by (timestamp, key).
            let mut counts: BTreeMap<(i64, String), usize> = BTreeMap::new();
            for r in &rows {
                let k = (r.timestamp_ms, r.partition_key.clone());
                *counts.entry(k).or_insert(0) += 1;
            }
            let dup = counts.iter().find(|(_, c)| **c != 1);
            assert!(
                dup.is_none(),
                "case={} run={} expected no duplicates; found {:?}",
                case.name,
                run_idx,
                dup
            );

            // Internal consistency check.
            for r in &rows {
                assert!(r.cnt >= 1, "cnt must be >= 1: {:?}", r);
                let exp_avg = r.sum / (r.cnt as f64);
                assert!(
                    (r.avg - exp_avg).abs() < 1e-9,
                    "avg mismatch row={:?}",
                    r
                );
            }

            let missed = total_records.saturating_sub(rows.len());
            println!(
                "bench case={} run={} produced={} expected={} missed={} (ooo_ms={} lateness_ms={} rate={:?})",
                case.name,
                run_idx,
                rows.len(),
                total_records,
                missed,
                case.out_of_orderness_ms,
                case.lateness_ms,
                case.rate
            );
            results.push(BenchRunResult {
                case_name: case.name.clone(),
                run_idx,
                produced: rows.len(),
                expected: total_records,
                missed,
                parallelism: case.parallelism,
                out_of_orderness_ms: case.out_of_orderness_ms,
                lateness_ms: case.lateness_ms,
                step_ms: case.step_ms,
                rate: case.rate,
            });
            misses.push(missed);
        }

        let min_missed = *misses.iter().min().unwrap_or(&0);
        let max_missed = *misses.iter().max().unwrap_or(&0);
        let sum_missed: usize = misses.iter().sum();
        let avg_missed = (sum_missed as f64) / (misses.len().max(1) as f64);
        let avg_drop_rate = avg_missed / (total_records as f64);

        summary.push(BenchSummary {
            case_name: case.name.clone(),
            min_missed,
            max_missed,
            avg_missed: sum_missed / misses.len().max(1),
            avg_drop_rate,
        });
    }

    println!("\n==== watermark benchmark summary ====");
    for s in &summary {
        println!(
            "case={} min_missed={} max_missed={} avg_missed={} avg_drop_rate={:.4}",
            s.case_name, s.min_missed, s.max_missed, s.avg_missed, s.avg_drop_rate
        );
    }

    let json_path = bench_output_path();
    write_bench_json(&json_path, total_records, runs, &results, &summary)?;
    println!("bench results written to: {:?}", json_path);

    Ok(())
}

