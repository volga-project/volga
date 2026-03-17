use std::sync::Arc;

use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::plain::PlainAggregation;
use crate::runtime::operators::window::aggregates::retractable::RetractableAggregation;
use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::aggregates::plain::CursorBounds;
use crate::runtime::operators::window::aggregates::{Aggregation, VirtualPoint};
use crate::storage::index::{BucketIndex, DataRequest, SortedRangeView};
use crate::runtime::operators::window::{Cursor, TimeGranularity};
use crate::storage::batch_store::BatchId;
use arrow::array::BooleanArray;

#[derive(Debug, Clone, Copy)]
struct Row {
    ts: i64,
    seq: u64,
    value: f64,
}

#[derive(Debug, Clone, Copy)]
enum Win {
    Range { length_ms: i64 },
    Rows { preceding: usize }, // window size = preceding + 1
}

fn win_sql_sum(win: Win) -> String {
    match win {
        Win::Range { length_ms } => format!(
            "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '{}' MILLISECOND PRECEDING AND CURRENT ROW
)",
            length_ms
        ),
        Win::Rows { preceding } => format!(
            "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  ROWS BETWEEN {} PRECEDING AND CURRENT ROW
)",
            preceding
        ),
    }
}

fn assert_f64s(vals: &[ScalarValue], expected: &[f64]) {
    assert_eq!(vals.len(), expected.len());
    for (i, (v, e)) in vals.iter().zip(expected.iter()).enumerate() {
        let ScalarValue::Float64(Some(got)) = v else {
            panic!("expected Float64 at {i}, got {v:?}");
        };
        assert!((got - e).abs() < 1e-9, "mismatch at {i}: got={got} expected={e}");
    }
}

fn sorted_rows(rows: &[Row]) -> Vec<Row> {
    let mut out = rows.to_vec();
    out.sort_by_key(|r| (r.ts, r.seq));
    out
}

fn bucketize(
    gran: TimeGranularity,
    rows: &[Row],
) -> (BucketIndex, Vec<(i64, arrow::record_batch::RecordBatch)>) {
    let mut idx = BucketIndex::new(gran);
    let mut by_bucket: std::collections::BTreeMap<i64, Vec<Row>> = std::collections::BTreeMap::new();
    for r in rows {
        by_bucket.entry(gran.start(r.ts)).or_default().push(*r);
    }

    let mut uid = 0u64;
    let mut buckets: Vec<(i64, arrow::record_batch::RecordBatch)> = Vec::new();
    for (bucket_ts, mut rs) in by_bucket {
        rs.sort_by_key(|r| (r.ts, r.seq));
        let batch_rows: Vec<(i64, f64, &str, u64)> = rs
            .iter()
            .map(|r| (r.ts, r.value, "A", r.seq))
            .collect();
        let batch = test_utils::batch(&batch_rows);
        let min = rs.first().unwrap();
        let max = rs.last().unwrap();
        idx.insert_batch(
            BatchId::new(0, bucket_ts, uid),
            Cursor::new(min.ts, min.seq),
            Cursor::new(max.ts, max.seq),
            rs.len(),
        );
        buckets.push((bucket_ts, batch));
        uid += 1;
    }

    (idx, buckets)
}

fn expected_sum_for_entry_rows(all_rows: &[Row], win: Win, entry_rows: &[Row]) -> Vec<f64> {
    let rows = sorted_rows(all_rows);
    let mut out = Vec::with_capacity(entry_rows.len());
    for e in entry_rows {
        let end = (e.ts, e.seq);
        let s = match win {
            Win::Range { length_ms } => {
                let start_ts = e.ts.saturating_sub(length_ms);
                rows.iter()
                    .filter(|r| r.ts >= start_ts && (r.ts, r.seq) <= end)
                    .map(|r| r.value)
                    .sum::<f64>()
            }
            Win::Rows { preceding } => {
                let window_size = preceding + 1;
                let mut prefix: Vec<&Row> = rows.iter().filter(|r| (r.ts, r.seq) <= end).collect();
                if prefix.len() > window_size {
                    prefix.drain(0..(prefix.len() - window_size));
                }
                prefix.into_iter().map(|r| r.value).sum::<f64>()
            }
        };
        out.push(s);
    }
    out
}

fn expected_sum_for_points(all_rows: &[Row], win: Win, points: &[(Row, bool)]) -> Vec<f64> {
    // points: (virtual_row, include_virtual)
    let rows = sorted_rows(all_rows);
    let mut out = Vec::with_capacity(points.len());
    for (p, include_virtual) in points {
        let end_ts = p.ts;
        let stored: Vec<&Row> = rows.iter().filter(|r| r.ts <= end_ts).collect();
        let sum_stored: f64 = match win {
            Win::Range { length_ms } => {
                let start_ts = end_ts.saturating_sub(length_ms);
                stored
                    .into_iter()
                    .filter(|r| r.ts >= start_ts)
                    .map(|r| r.value)
                    .sum::<f64>()
            }
            Win::Rows { preceding } => {
                let window_size = preceding + 1;
                let stored_take = if *include_virtual {
                    window_size.saturating_sub(1)
                } else {
                    window_size
                };
                let mut tail = stored;
                if tail.len() > stored_take {
                    tail.drain(0..(tail.len() - stored_take));
                }
                tail.into_iter().map(|r| r.value).sum::<f64>()
            }
        };
        out.push(sum_stored + if *include_virtual { p.value } else { 0.0 });
    }
    out
}

fn build_views_for_requests(
    gran: TimeGranularity,
    requests: &[DataRequest],
    buckets: &[(i64, arrow::record_batch::RecordBatch)],
    window_expr_for_args: &Arc<dyn WindowExpr>,
) -> Vec<SortedRangeView> {
    requests
        .iter()
        .map(|r| {
            // Only include buckets that fall inside the request range.
            let bs: Vec<_> = buckets
                .iter()
                .filter(|(ts, _)| *ts >= r.bucket_range.start && *ts <= r.bucket_range.end)
                .cloned()
                .collect();
            test_utils::make_view(gran, *r, bs, window_expr_for_args)
        })
        .collect()
}

fn points_with_args(window_expr: &Arc<dyn WindowExpr>, points: &[Row]) -> Vec<VirtualPoint> {
    points
        .iter()
        .map(|p| {
            let args = window_expr
                .evaluate_args(&test_utils::one_row_batch(p.ts, p.value, "A", p.seq))
                .expect("eval args");
            VirtualPoint {
                ts: p.ts,
                args: Some(Arc::new(args)),
            }
        })
        .collect()
}

#[tokio::test]
async fn plain_range_rows_and_range_multi_bucket() {
    // Data spans multiple buckets with uneven bucket occupancy.
    let stored = vec![
        Row { ts: 1000, seq: 0, value: 10.0 },
        Row { ts: 1500, seq: 1, value: 30.0 },
        Row { ts: 2000, seq: 2, value: 20.0 },
        Row { ts: 3200, seq: 3, value: 5.0 },
        Row { ts: 5200, seq: 4, value: 7.0 },
    ];
    let gran = TimeGranularity::Seconds(1);
    let (bucket_index, buckets) = bucketize(gran, &stored);

    for win in [Win::Range { length_ms: 2000 }, Win::Rows { preceding: 2 }] {
        let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;
        // Emit only rows up to (ts=3200, seq=3) -> excludes the last stored row at 5200.
        let bounds = CursorBounds {
            prev: None,
            new: Cursor::new(3200, 3),
        };

        let entry_rows: Vec<Row> = stored
            .iter()
            .copied()
            .filter(|r| Cursor::new(r.ts, r.seq) <= bounds.new)
            .collect();
        let expected = expected_sum_for_entry_rows(&stored, win, &entry_rows);

        let agg = PlainAggregation::for_range(
            &bucket_index,
            window_expr.clone(),
            None,
            bounds,
        );
        let requests = agg.get_data_requests();
        let views = build_views_for_requests(gran, &requests, &buckets, &window_expr);
        let vals = agg.produce_aggregates_from_ranges(&views, None).await.values;
        assert_f64s(&vals, &expected);
    }
}

#[tokio::test]
async fn plain_points_overlaps_merge_range_and_rows() {
    let stored = vec![
        Row { ts: 1000, seq: 0, value: 10.0 },
        Row { ts: 1500, seq: 1, value: 30.0 },
        Row { ts: 2000, seq: 2, value: 20.0 },
        Row { ts: 2500, seq: 3, value: 1.0 },
        Row { ts: 3200, seq: 4, value: 5.0 },
    ];
    let gran = TimeGranularity::Seconds(1);
    let (bucket_index, buckets) = bucketize(gran, &stored);

    // Two points close enough that their window ranges overlap -> requests should merge to 1 for both ROWS and RANGE.
    let points = vec![
        Row { ts: 2600, seq: 10, value: 2.0 },
        Row { ts: 2700, seq: 11, value: 3.0 },
    ];

    for win in [Win::Range { length_ms: 2000 }, Win::Rows { preceding: 2 }] {
        let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;
        let agg = PlainAggregation::for_points(
            points_with_args(&window_expr, &points),
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );

        let requests = agg.get_data_requests();
        assert_eq!(requests.len(), 1, "overlapping points should merge requests");
        let views = build_views_for_requests(gran, &requests, &buckets, &window_expr);

        let include_virtual = true;
        let expected_points: Vec<(Row, bool)> = points.iter().copied().map(|p| (p, include_virtual)).collect();
        let expected = expected_sum_for_points(&stored, win, &expected_points);

        let vals = agg.produce_aggregates_from_ranges(&views, None).await.values;
        assert_f64s(&vals, &expected);
    }
}

#[tokio::test]
async fn plain_points_disjoint_ranges_produce_multiple_requests() {
    let stored = vec![
        Row { ts: 1000, seq: 0, value: 10.0 },
        Row { ts: 2000, seq: 1, value: 20.0 },
        Row { ts: 9000, seq: 2, value: 1.0 },
        Row { ts: 9500, seq: 3, value: 2.0 },
    ];
    let gran = TimeGranularity::Seconds(1);
    let (bucket_index, buckets) = bucketize(gran, &stored);

    let points = vec![
        Row { ts: 2500, seq: 10, value: 3.0 },
        Row { ts: 9800, seq: 11, value: 4.0 },
    ];

    let win = Win::Range { length_ms: 1000 };
    let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;
    let agg = PlainAggregation::for_points(
        points_with_args(&window_expr, &points),
        &bucket_index,
        window_expr.clone(),
        None,
        false,
    );

    let requests = agg.get_data_requests();
    assert_eq!(requests.len(), 2, "far points should not be merged");
    let views = build_views_for_requests(gran, &requests, &buckets, &window_expr);

    let include_virtual = true;
    let expected_points: Vec<(Row, bool)> = points.iter().copied().map(|p| (p, include_virtual)).collect();
    let expected = expected_sum_for_points(&stored, win, &expected_points);

    let vals = agg.produce_aggregates_from_ranges(&views, None).await.values;
    assert_f64s(&vals, &expected);
}

#[tokio::test]
async fn plain_points_empty_storage_returns_virtual_only() {
    let stored: Vec<Row> = vec![];
    let gran = TimeGranularity::Seconds(2);
    let (bucket_index, buckets) = bucketize(gran, &stored);

    let points = vec![Row { ts: 1500, seq: 1, value: 42.0 }];
    for win in [Win::Range { length_ms: 2000 }, Win::Rows { preceding: 5 }] {
        let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;
        let agg = PlainAggregation::for_points(
            points_with_args(&window_expr, &points),
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );
        let requests = agg.get_data_requests();
        let views = build_views_for_requests(gran, &requests, &buckets, &window_expr);

        let expected_points = vec![(points[0], true)];
        let expected = expected_sum_for_points(&stored, win, &expected_points);

        let vals = agg.produce_aggregates_from_ranges(&views, None).await.values;
        assert_f64s(&vals, &expected);
    }
}

#[tokio::test]
async fn retractable_range_matches_bruteforce_for_updates() {
    // Two-step streaming update for both window types.
    let gran = TimeGranularity::Seconds(1);
    for win in [Win::Range { length_ms: 2000 }, Win::Rows { preceding: 2 }] {
        let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;

        // Step1 stored rows.
        let step1 = vec![
            Row { ts: 1000, seq: 0, value: 10.0 },
            Row { ts: 1500, seq: 1, value: 30.0 },
            Row { ts: 2000, seq: 2, value: 20.0 },
        ];
        let (mut bucket_index, buckets1) = bucketize(gran, &step1);

        let new1 = bucket_index.max_pos_seen();
        let agg1 = RetractableAggregation::from_range(
            0,
            None,
            new1,
            &bucket_index,
            window_expr.clone(),
            None,
            0,
            gran,
        );
        let reqs1 = agg1.get_data_requests();
        let views1 = build_views_for_requests(gran, &reqs1, &buckets1, &window_expr);
        let res1 = agg1.produce_aggregates_from_ranges(&views1, None).await;
        let vals1 = res1.values;
        let state1 = res1.accumulator_state;

        let entry_rows1: Vec<Row> = step1
            .iter()
            .copied()
            .filter(|r| Cursor::new(r.ts, r.seq) <= new1)
            .collect();
        let expected1 = expected_sum_for_entry_rows(&step1, win, &entry_rows1);
        assert_f64s(&vals1, &expected1);

        // Step2 add rows.
        let step2_new = vec![
            Row { ts: 3200, seq: 3, value: 5.0 },
            Row { ts: 5200, seq: 4, value: 7.0 },
        ];
        for r in &step2_new {
            let bucket_ts = gran.start(r.ts);
            bucket_index.insert_batch(
                BatchId::new(0, bucket_ts, 100 + r.seq),
                Cursor::new(r.ts, r.seq),
                Cursor::new(r.ts, r.seq),
                1,
            );
        }
        let all2: Vec<Row> = step1.iter().copied().chain(step2_new.iter().copied()).collect();
        let (_idx2, buckets2) = bucketize(gran, &all2);

        let prev = Some(new1);
        let new2 = bucket_index.max_pos_seen();
        let agg2 = RetractableAggregation::from_range(
            0,
            prev,
            new2,
            &bucket_index,
            window_expr.clone(),
            state1,
            0,
            gran,
        );
        let reqs2 = agg2.get_data_requests();
        let views2 = build_views_for_requests(gran, &reqs2, &buckets2, &window_expr);
        let vals2 = agg2.produce_aggregates_from_ranges(&views2, None).await.values;

        let entry_rows2: Vec<Row> = all2
            .iter()
            .copied()
            .filter(|r| Cursor::new(r.ts, r.seq) > prev.unwrap())
            .filter(|r| Cursor::new(r.ts, r.seq) <= new2)
            .collect();
        let expected2 = expected_sum_for_entry_rows(&all2, win, &entry_rows2);
        assert_f64s(&vals2, &expected2);
    }
}

#[tokio::test]
async fn retractable_points_matches_bruteforce_rows_and_range() {
    let stored = vec![
        Row { ts: 1000, seq: 0, value: 10.0 },
        Row { ts: 1500, seq: 1, value: 30.0 },
        Row { ts: 2000, seq: 2, value: 20.0 },
        Row { ts: 2500, seq: 3, value: 1.0 },
    ];
    let gran = TimeGranularity::Seconds(1);
    let (bucket_index, buckets) = bucketize(gran, &stored);

    // Base is "as of" processed_pos.
    let processed_pos = Cursor::new(2000, 2);
    let base_batch = test_utils::batch(&[
        (1000, 10.0, "A", 0),
        (1500, 30.0, "A", 1),
        (2000, 20.0, "A", 2),
    ]);

    for win in [Win::Range { length_ms: 2000 }, Win::Rows { preceding: 2 }] {
        let window_expr = test_utils::window_expr_from_sql(&win_sql_sum(win)).await;
        let mut acc = match crate::runtime::operators::window::aggregates::create_window_aggregator(&window_expr) {
            crate::runtime::operators::window::aggregates::WindowAggregator::Accumulator(a) => a,
            _ => panic!("not supported"),
        };
        let args = window_expr.evaluate_args(&base_batch).expect("eval args");
        acc.update_batch(&args).expect("update_batch failed");
        let base_state = acc.state().expect("state failed");

        let points = vec![
            Row { ts: 2200, seq: 10, value: 2.0 },
            Row { ts: 3200, seq: 11, value: 5.0 },
        ];

        // Retractable points require non-late points (>= processed_pos.ts).
        let agg = RetractableAggregation::for_points(
            points_with_args(&window_expr, &points),
            &bucket_index,
            window_expr.clone(),
            Some(processed_pos),
            Some(base_state.clone()),
            false,
        );

        let requests = agg.get_data_requests();
        // Retractable points loads one base view.
        assert_eq!(requests.len(), 1);
        let views = build_views_for_requests(gran, &requests, &buckets, &window_expr);

        let expected_points: Vec<(Row, bool)> = points.iter().copied().map(|p| (p, true)).collect();
        let expected = expected_sum_for_points(&stored, win, &expected_points);

        let vals = agg.produce_aggregates_from_ranges(&views, None).await.values;
        assert_f64s(&vals, &expected);
    }
}

#[tokio::test]
async fn point_request_merge_is_only_for_matching_units() {
    // Sanity: ensure we never accidentally mix units within a single SQL.
    let range_expr = test_utils::window_expr_from_sql(&win_sql_sum(Win::Range { length_ms: 2000 })).await;
    assert_eq!(range_expr.get_window_frame().units, WindowFrameUnits::Range);
    let rows_expr = test_utils::window_expr_from_sql(&win_sql_sum(Win::Rows { preceding: 2 })).await;
    assert_eq!(rows_expr.get_window_frame().units, WindowFrameUnits::Rows);
}

