//! Correctness matrix: absolute RANGE oracle + tiling path equivalence.
//!
//! For each tiling config (off / 1m / 1m+5m), emits must match a pure oracle that
//! mirrors WO RANGE semantics (`ts >= end.ts − wl` through end cursor, inclusive).
//! Covers retractable (sum/count/avg) + plain (min/max), multi-window lengths,
//! unaligned/duplicate-ts edges, and a two-watermark warm-slide scenario.
//!
//! WRO: thin point oracle on the same fixture (tiling off + 1m; points before / at /
//! after `processed_pos`; exclude current row on/off).

use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::tests::harness::{
    assert_window_values, batch, run_wo, run_wo_scenario, test_input_schema, window_exec_from_sql,
    Harness, WoWroHarness,
};
use crate::runtime::operators::window::window_operator::WindowOperatorConfig;
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::{TileConfig, TimeGranularity};

#[derive(Clone, Copy, Debug)]
struct Ev {
    ts: i64,
    seq: u64,
    value: f64,
}

/// Fixture: unaligned starts, 5m boundaries, duplicate ts (seq order matters).
fn fixture_events() -> Vec<Ev> {
    // seq assigned in ingest order (single batch → 0..n-1).
    let rows = [
        (30_000, 1.0),
        (60_000, 2.0),
        (60_000, 2.5), // duplicate ts
        (120_000, 3.0),
        (300_000, 4.0),
        (330_000, 5.0),
        (420_000, 6.0),
        (600_000, 7.0),
        (720_000, 8.0),
    ];
    rows.iter()
        .enumerate()
        .map(|(i, &(ts, value))| Ev {
            ts,
            seq: i as u64,
            value,
        })
        .collect()
}

fn events_batch(evs: &[Ev]) -> arrow::record_batch::RecordBatch {
    batch(
        evs.iter().map(|e| e.ts).collect(),
        evs.iter().map(|e| e.value).collect(),
        vec!["A"; evs.len()],
    )
}

fn matrix_sql() -> &'static str {
    // Column order = oracle window order.
    // w7: retractable sum/count/avg; w3: plain min; w5: plain max.
    r#"SELECT timestamp, value, partition_key,
  SUM(value) OVER w7 as sum_7m,
  COUNT(value) OVER w7 as cnt_7m,
  AVG(value) OVER w7 as avg_7m,
  MIN(value) OVER w3 as min_3m,
  MAX(value) OVER w5 as max_5m
FROM test_table
WINDOW
  w7 AS (
    PARTITION BY partition_key
    ORDER BY timestamp
    RANGE BETWEEN INTERVAL '7' MINUTE PRECEDING AND CURRENT ROW
  ),
  w3 AS (
    PARTITION BY partition_key
    ORDER BY timestamp
    RANGE BETWEEN INTERVAL '3' MINUTE PRECEDING AND CURRENT ROW
  ),
  w5 AS (
    PARTITION BY partition_key
    ORDER BY timestamp
    RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
  )"#
}

const WL7: i64 = 7 * 60_000;
const WL3: i64 = 3 * 60_000;
const WL5: i64 = 5 * 60_000;

/// Same membership as plain eval: first idx with `ts >= end.ts − wl` through `end_idx`.
fn window_slice<'a>(evs: &'a [Ev], end_idx: usize, wl: i64) -> &'a [Ev] {
    let end = &evs[end_idx];
    let start_ts = end.ts.saturating_sub(wl);
    let start = (0..=end_idx)
        .find(|&i| evs[i].ts >= start_ts)
        .unwrap_or(end_idx);
    &evs[start..=end_idx]
}

fn oracle_row(evs: &[Ev], end_idx: usize) -> Vec<ScalarValue> {
    let s7 = window_slice(evs, end_idx, WL7);
    let s3 = window_slice(evs, end_idx, WL3);
    let s5 = window_slice(evs, end_idx, WL5);
    let sum7: f64 = s7.iter().map(|e| e.value).sum();
    let cnt7 = s7.len() as i64;
    let avg7 = sum7 / cnt7 as f64;
    let min3 = s3.iter().map(|e| e.value).fold(f64::INFINITY, f64::min);
    let max5 = s5.iter().map(|e| e.value).fold(f64::NEG_INFINITY, f64::max);
    vec![
        ScalarValue::Float64(Some(sum7)),
        ScalarValue::Int64(Some(cnt7)),
        ScalarValue::Float64(Some(avg7)),
        ScalarValue::Float64(Some(min3)),
        ScalarValue::Float64(Some(max5)),
    ]
}

/// Expected window cols for every event with `ts <= wm` (emit order = event order).
fn oracle_through_wm(evs: &[Ev], wm: i64) -> Vec<Vec<ScalarValue>> {
    evs.iter()
        .enumerate()
        .filter(|(_, e)| e.ts <= wm)
        .map(|(i, _)| oracle_row(evs, i))
        .collect()
}

fn tiling_variants() -> Vec<(&'static str, Option<TileConfig>)> {
    vec![
        ("no_tiling", None),
        (
            "1m",
            Some(TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap()),
        ),
        (
            "1m+5m",
            Some(
                TileConfig::new(vec![
                    TimeGranularity::Minutes(1),
                    TimeGranularity::Minutes(5),
                ])
                .unwrap(),
            ),
        ),
    ]
}

#[tokio::test]
async fn matrix_single_wm_matches_oracle() {
    let evs = fixture_events();
    let sql = matrix_sql();
    let input_cols = test_input_schema().fields().len();
    let wm = 720_000i64;
    let expected = oracle_through_wm(&evs, wm);
    let events = events_batch(&evs);

    // Plan SQL once; only tiling varies across runs.
    let exec = window_exec_from_sql(sql).await;

    for (label, tiling) in tiling_variants() {
        let mut cfg = WindowOperatorConfig::new(exec.clone());
        if tiling.is_some() {
            cfg.spec = WindowOperatorSpec {
                tiling,
                ..Default::default()
            };
        }
        let mut h = Harness::new(cfg).await;
        h.ingest(events.clone(), "A").await;
        let out = h.watermark_and_output(wm as u64).await;
        let _ = h.drain_passthrough_watermark().await;
        assert_window_values(&out, input_cols, &expected, label);
    }
}

#[tokio::test]
async fn matrix_two_wm_warm_slide_matches_oracle() {
    // First WM mid-stream → cold/warm within advance; second WM → retractable warm slide.
    let evs = fixture_events();
    let sql = matrix_sql();
    let input_cols = test_input_schema().fields().len();
    let wm1 = 420_000i64;
    let wm2 = 720_000i64;

    let split = evs.iter().position(|e| e.ts > wm1).unwrap();
    let batch1 = events_batch(&evs[..split]);
    let batch2 = events_batch(&evs[split..]);

    // Oracle: all events still use global seq/order; emits are events with ts<=wm1 then wm1<ts<=wm2.
    let mut expected = oracle_through_wm(&evs, wm1);
    expected.extend(
        evs.iter()
            .enumerate()
            .filter(|(_, e)| e.ts > wm1 && e.ts <= wm2)
            .map(|(i, _)| oracle_row(&evs, i)),
    );

    for (label, tiling) in tiling_variants() {
        let out = run_wo_scenario(
            sql,
            tiling,
            "A",
            &[(batch1.clone(), wm1 as u64), (batch2.clone(), wm2 as u64)],
        )
        .await;
        assert_window_values(&out, input_cols, &expected, label);
    }
}

#[tokio::test]
async fn matrix_short_window_all_raw_matches_oracle() {
    // wl ≪ min gran → planner all-raw; still must match oracle under tiling.
    let evs = fixture_events();
    let sql = r#"SELECT timestamp, value, partition_key,
  SUM(value) OVER w as sum_30s,
  MIN(value) OVER w as min_30s
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '30000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let input_cols = test_input_schema().fields().len();
    let wm = 720_000i64;
    let wl = 30_000i64;
    let expected: Vec<Vec<ScalarValue>> = evs
        .iter()
        .enumerate()
        .filter(|(_, e)| e.ts <= wm)
        .map(|(i, _)| {
            let s = window_slice(&evs, i, wl);
            let sum: f64 = s.iter().map(|e| e.value).sum();
            let min = s.iter().map(|e| e.value).fold(f64::INFINITY, f64::min);
            vec![
                ScalarValue::Float64(Some(sum)),
                ScalarValue::Float64(Some(min)),
            ]
        })
        .collect();
    let events = events_batch(&evs);
    let tiling_5m = TileConfig::new(vec![TimeGranularity::Minutes(5)]).unwrap();

    for (label, tiling) in [("no_tiling", None), ("5m_idle", Some(tiling_5m.clone()))] {
        let out = run_wo(sql, tiling, events.clone(), "A", wm as u64).await;
        assert_window_values(&out, input_cols, &expected, label);
    }
}

/// Store members for a WRO point: always `ts ∈ [point_ts − wl, point_ts]`.
/// Exclude only skips folding in `point_value` (request args), not store@T.
fn point_store<'a>(evs: &'a [Ev], point_ts: i64, wl: i64) -> Vec<&'a Ev> {
    evs.iter()
        .filter(|e| e.ts >= point_ts.saturating_sub(wl) && e.ts <= point_ts)
        .collect()
}

fn oracle_wro_point(evs: &[Ev], point_ts: i64, point_value: f64, exclude: bool) -> Vec<ScalarValue> {
    let request = (!exclude).then_some(point_value);
    let agg = |wl: i64, kind: &str| -> ScalarValue {
        let store = point_store(evs, point_ts, wl);
        let mut vals: Vec<f64> = store.iter().map(|e| e.value).collect();
        if let Some(v) = request {
            vals.push(v);
        }
        if vals.is_empty() {
            return ScalarValue::Null;
        }
        match kind {
            "sum" => ScalarValue::Float64(Some(vals.iter().sum())),
            "cnt" => ScalarValue::Int64(Some(vals.len() as i64)),
            "avg" => {
                let n = vals.len() as f64;
                ScalarValue::Float64(Some(vals.iter().sum::<f64>() / n))
            }
            "min" => ScalarValue::Float64(Some(vals.iter().copied().fold(f64::INFINITY, f64::min))),
            "max" => {
                ScalarValue::Float64(Some(vals.iter().copied().fold(f64::NEG_INFINITY, f64::max)))
            }
            _ => unreachable!(),
        }
    };
    vec![
        agg(WL7, "sum"),
        agg(WL7, "cnt"),
        agg(WL7, "avg"),
        agg(WL3, "min"),
        agg(WL5, "max"),
    ]
}

#[tokio::test]
async fn matrix_wro_points_match_oracle() {
    // After WO advance to last fixture ts: P = 720_000.
    // Points probe rebuild (T < P), acc-only / jump (T == P), and leave+add jump (T > P).
    let evs = fixture_events();
    let sql = matrix_sql();
    let input_cols = test_input_schema().fields().len();
    let wm = 720_000i64;
    let events = events_batch(&evs);

    // (ts, value): mid gap, at P (same value as last event), after P.
    let points = [
        (350_000i64, 50.0),
        (720_000i64, 8.0),
        (800_000i64, 9.0),
    ];
    let request_batch = batch(
        points.iter().map(|(t, _)| *t).collect(),
        points.iter().map(|(_, v)| *v).collect(),
        vec!["A"; points.len()],
    );

    let tiling_variants = [
        ("no_tiling", None),
        (
            "1m",
            Some(TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap()),
        ),
    ];

    for (tiling_label, tiling) in tiling_variants {
        for exclude in [false, true] {
            let label = format!("{tiling_label}_exclude_{exclude}");
            let expected: Vec<Vec<ScalarValue>> = points
                .iter()
                .map(|(ts, v)| oracle_wro_point(&evs, *ts, *v, exclude))
                .collect();

            let mut h = WoWroHarness::new(sql, tiling.clone(), exclude).await;
            h.ingest(events.clone(), "A").await;
            h.advance(wm as u64).await;
            let out = h.request(request_batch.clone(), "A").await;
            assert_window_values(&out, input_cols, &expected, &label);
        }
    }
}
