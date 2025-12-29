use crate::runtime::operators::window::index::SortedRangeIndex;
use crate::runtime::operators::window::index::RowPtr;
use crate::runtime::operators::window::tiles::{Tile, Tiles};
use crate::runtime::operators::window::Cursor;
use crate::storage::batch_store::Timestamp;

#[derive(Debug, Clone, Copy)]
pub enum WindowSpec {
    Rows { size: usize },
    Range { length_ms: i64 },
}

#[derive(Debug, Clone)]
pub struct TiledSplit {
    pub front_end: RowPtr,
    pub tiles: Vec<Tile>,
    pub back_start: RowPtr,
}

/// Compute window start for a given window end position (no bucket-range clamping).
pub fn window_start_unclamped(idx: &SortedRangeIndex, end: RowPtr, spec: WindowSpec) -> RowPtr {
    match spec {
        WindowSpec::Rows { size } => idx.pos_n_rows(&end, size.saturating_sub(1), true),
        WindowSpec::Range { length_ms } => {
            let end_ts = idx.get_timestamp(&end);
            let start_ts = end_ts.saturating_sub(length_ms);
            idx.seek_ts_ge(start_ts).unwrap_or_else(|| idx.first_pos())
        }
    }
}

/// Clamp a start position to be within the window bucket range.
pub fn clamp_start_to_bucket(start: RowPtr, min_bucket_ts: Timestamp) -> RowPtr {
    if start.bucket_ts < min_bucket_ts {
        RowPtr::new(min_bucket_ts, 0)
    } else {
        start
    }
}

/// Compute tiled split for a window [start,end] (both inclusive).
///
/// Returns None when tiling doesn't help (no full tiles strictly inside the window).
pub fn tiled_split(
    idx: &SortedRangeIndex,
    start: RowPtr,
    end: RowPtr,
    tiles: &Tiles,
) -> Option<TiledSplit> {
    let start_ts = idx.get_timestamp(&start);
    let end_ts = idx.get_timestamp(&end);

    // Avoid boundary timestamps because duplicates may exist at start/end.
    let tiles_start_ts = start_ts + 1;
    let tiles_end_ts = end_ts - 1;
    if tiles_start_ts > tiles_end_ts {
        return None;
    }

    let tiles_mid = tiles.get_tiles_for_range(tiles_start_ts, tiles_end_ts);
    if tiles_mid.is_empty() {
        return None;
    }

    let first_tile_start_ts = tiles_mid[0].tile_start;
    let last_tile_end_ts = tiles_mid[tiles_mid.len() - 1].tile_end;

    // front_end: last row strictly before first tile start ts
    let first_tile_pos = idx.seek_ts_ge(first_tile_start_ts)?;
    let front_end = idx.prev_pos(first_tile_pos)?;

    // back_start: first row at/after last tile end ts
    //
    // Tiles cover [tile_start, tile_end) (half-open). A row with ts == tile_end belongs to the next
    // tile and must be accounted for in the "back" segment. Using `gt` would skip it.
    let back_start = idx.seek_ts_ge(last_tile_end_ts)?;

    if front_end < start || back_start > end {
        return None;
    }

    Some(TiledSplit {
        front_end,
        tiles: tiles_mid,
        back_start,
    })
}

/// Find the first update position for retractable aggregations.
pub fn first_update_pos(idx: &SortedRangeIndex, prev_processed_until: Option<Cursor>) -> Option<RowPtr> {
    match prev_processed_until {
        None => Some(idx.first_pos()),
        Some(p) => {
            let first = idx.get_row_pos(&idx.first_pos());
            if p < first {
                Some(idx.first_pos())
            } else {
                idx.seek_rowpos_gt(p)
            }
        }
    }
}

/// ROWS retract start position logic extracted from `retractable.rs`.
pub fn initial_retract_pos_rows(
    retracts: &SortedRangeIndex,
    updates: &SortedRangeIndex,
    update_pos: RowPtr,
    window_size: usize,
    row_distance: usize,
    num_updates: usize,
) -> RowPtr {
    let update_start_offset = updates.count_between(&updates.first_pos(), &update_pos) - 1;
    let retract_end_offset = window_size.saturating_sub(row_distance + update_start_offset);
    let retract_start_offset = retract_end_offset + num_updates;
    retracts.pos_from_end(retract_start_offset)
}

/// RANGE retract start position logic extracted from `retractable.rs`.
pub fn initial_retract_pos_range(
    retracts: &SortedRangeIndex,
    prev_processed_until: Cursor,
    window_length_ms: i64,
) -> RowPtr {
    let prev_start_ts = prev_processed_until.ts - window_length_ms;
    retracts
        .seek_ts_ge(prev_start_ts)
        .unwrap_or_else(|| retracts.first_pos())
}

#[cfg(test)]
mod tiled_split_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::WindowFrameUnits;

    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::aggregates::BucketRange;
    use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeBucket, SortedRangeIndex, SortedRangeView};
    use crate::runtime::operators::window::tiles::{TileConfig, TimeGranularity as TileGranularity, Tiles};
    use crate::runtime::operators::window::{Cursor, RowPtr};

    use super::tiled_split;

    fn make_view(
        gran: TileGranularity,
        bucket_range: BucketRange,
        buckets: Vec<(i64, RecordBatch)>,
    ) -> SortedRangeView {
        let request = DataRequest {
            bucket_range,
            bounds: DataBounds::All,
        };
        let mut out: HashMap<i64, SortedRangeBucket> = HashMap::new();
        for (bucket_ts, b) in buckets {
            out.insert(bucket_ts, SortedRangeBucket::new(bucket_ts, b, 0, 3, Arc::new(vec![])));
        }
        SortedRangeView::new(
            request,
            gran,
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            out,
        )
    }

    #[tokio::test]
    async fn tiled_split_respects_half_open_tile_end_boundary() {
        // We want a case where the last selected tile ends exactly at an existing row timestamp
        // so `back_start` must use `>= tile_end`, not `> tile_end`.
        //
        // With 1s tiles and a window [0..5000] (inclusive), the mid tiles selected are:
        // [1000..2000), [2000..3000), [3000..4000), so last_tile_end_ts == 4000.
        // back_start must point to the row at ts=4000 (not 5000).

        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '4000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        assert_eq!(window_expr.get_window_frame().units, WindowFrameUnits::Range);

        let gran = TileGranularity::Seconds(1);
        let rows: Vec<(i64, f64, &str, u64)> = vec![
            (0, 1.0, "A", 0),
            (1000, 2.0, "A", 1),
            (2000, 3.0, "A", 2),
            (3000, 4.0, "A", 3),
            (4000, 5.0, "A", 4),
            (5000, 6.0, "A", 5),
        ];

        // Build tiles from the full batch.
        let all = test_utils::batch(&rows);
        let mut tiles = Tiles::new(
            TileConfig::new(vec![TileGranularity::Seconds(1)]).expect("tile config"),
            window_expr,
        );
        tiles.add_batch(&all, 0);

        // Build a view with one row per bucket (bucket_ts == ts for 1s granularity).
        let buckets: Vec<(i64, RecordBatch)> = rows
            .iter()
            .map(|(ts, v, pk, seq)| (*ts, test_utils::batch(&[(*ts, *v, *pk, *seq)])))
            .collect();
        let view = make_view(gran, BucketRange::new(0, 5000), buckets);
        let idx = SortedRangeIndex::new(&view);

        let start = RowPtr::new(0, 0);
        let end = RowPtr::new(5000, 0);
        let split = tiled_split(&idx, start, end, &tiles).expect("should split");

        assert_eq!(split.front_end, RowPtr::new(0, 0));
        assert_eq!(split.back_start, RowPtr::new(4000, 0));
        assert_eq!(split.tiles.len(), 3);
        assert_eq!(split.tiles[0].tile_start, 1000);
        assert_eq!(split.tiles[1].tile_start, 2000);
        assert_eq!(split.tiles[2].tile_start, 3000);
    }
}

#[cfg(test)]
mod window_logic_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use crate::runtime::operators::window::tiles::TimeGranularity;
    use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeBucket, SortedRangeIndex, SortedRangeView};
    use crate::runtime::operators::window::Cursor;

    use super::*;

    fn make_bucket(bucket_ts: i64, rows: &[(i64, u64, i64)]) -> SortedRangeBucket {
        let ts: TimestampMillisecondArray =
            rows.iter().map(|(ts, _, _)| *ts).collect::<Vec<_>>().into();
        let seq: UInt64Array = rows.iter().map(|(_, seq, _)| *seq).collect::<Vec<_>>().into();
        let val: Int64Array = rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>().into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("seq", DataType::UInt64, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts) as ArrayRef,
                Arc::new(seq) as ArrayRef,
                Arc::new(val.clone()) as ArrayRef,
            ],
        )
        .expect("record batch");

        let args = Arc::new(vec![Arc::new(val) as ArrayRef]);
        SortedRangeBucket::new(bucket_ts, batch, 0, 1, args)
    }

    #[test]
    fn test_window_start_unclamped_rows() {
        let mut buckets = HashMap::new();
        buckets.insert(1000, make_bucket(1000, &[(1000, 1, 10), (1500, 2, 11)]));
        buckets.insert(2000, make_bucket(2000, &[(2000, 1, 20), (2500, 2, 21)]));

        let req = DataRequest {
            bucket_range: crate::runtime::operators::window::aggregates::BucketRange::new(1000, 2000),
            bounds: DataBounds::All,
        };
        let view = SortedRangeView::new(
            req,
            TimeGranularity::Seconds(1),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            buckets,
        );
        let idx = SortedRangeIndex::new(&view);

        let end = RowPtr::new(2000, 0);
        let start = window_start_unclamped(&idx, end, WindowSpec::Rows { size: 2 });
        assert_eq!(start.bucket_ts, 1000);
        assert_eq!(start.row, 1);
    }

    #[test]
    fn test_clamp_start_to_bucket() {
        let s = RowPtr::new(1000, 1);
        let clamped = clamp_start_to_bucket(s, 2000);
        assert_eq!(clamped.bucket_ts, 2000);
        assert_eq!(clamped.row, 0);
    }

    // Note: `tiled_split` is tested indirectly in `Tiles` module tests; creating a `Tiles`
    // instance requires a DataFusion `WindowExpr`, which is intentionally kept out of these unit tests.
}


