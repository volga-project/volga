use crate::runtime::operators::window::index::RowIndex;
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
pub fn window_start_unclamped(idx: &RowIndex, end: RowPtr, spec: WindowSpec) -> RowPtr {
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
pub fn tiled_split(idx: &RowIndex, start: RowPtr, end: RowPtr, tiles: &Tiles) -> Option<TiledSplit> {
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

    // back_start: first row strictly after last tile end ts
    let back_start = idx.seek_ts_gt(last_tile_end_ts)?;

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
pub fn first_update_pos(idx: &RowIndex, prev_processed_until: Option<Cursor>) -> Option<RowPtr> {
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
    retracts: &RowIndex,
    updates: &RowIndex,
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
    retracts: &RowIndex,
    prev_processed_until: Cursor,
    window_length_ms: i64,
) -> RowPtr {
    let prev_start_ts = prev_processed_until.ts - window_length_ms;
    retracts
        .seek_ts_ge(prev_start_ts)
        .unwrap_or_else(|| retracts.first_pos())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use crate::runtime::operators::window::index::SortedBucketView;
    use crate::runtime::operators::window::tiles::TimeGranularity;

    use super::*;

    fn make_view(bucket_ts: i64, rows: &[(i64, u64, i64)], window_id: usize) -> SortedBucketView {
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

        let mut args: HashMap<usize, Arc<Vec<ArrayRef>>> = HashMap::new();
        args.insert(window_id, Arc::new(vec![Arc::new(val) as ArrayRef]));

        SortedBucketView::new(bucket_ts, batch, 0, 1, args)
    }

    #[test]
    fn test_window_start_unclamped_rows() {
        let mut views = HashMap::new();
        views.insert(1000, make_view(1000, &[(1000, 1, 10), (1500, 2, 11)], 0));
        views.insert(2000, make_view(2000, &[(2000, 1, 20), (2500, 2, 21)], 0));
        let idx = RowIndex::new(crate::runtime::operators::window::aggregates::BucketRange::new(1000, 2000), &views, 0, TimeGranularity::Seconds(1));

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

