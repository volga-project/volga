use std::collections::HashMap;

use arrow::array::ArrayRef;

use crate::runtime::operators::window::index::SortedBucketView;
use crate::runtime::operators::window::aggregates::BucketRange;
use crate::runtime::operators::window::Cursor;
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::storage::batch_store::Timestamp;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct RowPtr {
    pub bucket_ts: Timestamp,
    pub row: usize,
}

impl RowPtr {
    pub fn new(bucket_ts: Timestamp, row: usize) -> Self {
        Self { bucket_ts, row }
    }
}

#[derive(Clone, Copy, Debug)]
enum SeekBound {
    Ge,
    Gt,
    Eq,
}

/// A lightweight per-aggregation view over bucket data.
/// Holds a reference to shared bucket views and provides efficient position ops.
pub struct RowIndex<'a> {
    bucket_start_ts: Timestamp, // start bucket
    bucket_end_ts: Timestamp,   // end bucket
    window_id: usize,
    bucket_granularity: TimeGranularity,
    sorted_buckets_view: &'a HashMap<Timestamp, SortedBucketView>,
}

impl<'a> RowIndex<'a> {
    pub fn bucket_view(&self, bucket_ts: &Timestamp) -> Option<&SortedBucketView> {
        self.sorted_buckets_view.get(bucket_ts)
    }

    pub fn is_empty(&self) -> bool {
        self.sorted_buckets_view.is_empty()
    }

    pub fn new(
        range: BucketRange,
        sorted_buckets_view: &'a HashMap<Timestamp, SortedBucketView>,
        window_id: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
        // Invariant: range endpoints are bucket-aligned and the backing map contains
        // every bucket in the interval (contiguous bucket coverage).
        debug_assert_eq!(
            bucket_granularity.start(range.start),
            range.start,
            "RowIndex range.start must be bucket-aligned"
        );
        debug_assert_eq!(
            bucket_granularity.start(range.end),
            range.end,
            "RowIndex range.end must be bucket-aligned"
        );
        debug_assert!(
            range.start <= range.end,
            "RowIndex range must satisfy start <= end"
        );

        let mut ts = range.start;
        while ts <= range.end {
            assert!(
                sorted_buckets_view.contains_key(&ts),
                "RowIndex requires contiguous buckets; missing bucket_ts={ts} in views"
            );
            ts = bucket_granularity.next_start(ts);
        }

        Self {
            bucket_start_ts: range.start,
            bucket_end_ts: range.end,
            window_id,
            bucket_granularity,
            sorted_buckets_view,
        }
    }

    pub fn first_pos(&self) -> RowPtr {
        RowPtr::new(self.bucket_start_ts, 0)
    }

    pub fn last_pos(&self) -> RowPtr {
        let last_bucket = self
            .sorted_buckets_view
            .get(&self.bucket_end_ts)
            .expect("should exist");
        RowPtr::new(self.bucket_end_ts, last_bucket.size().saturating_sub(1))
    }

    pub fn get_timestamp(&self, pos: &RowPtr) -> Timestamp {
        self.sorted_buckets_view
            .get(&pos.bucket_ts)
            .expect("should exist")
            .timestamp(pos.row)
    }

    pub fn get_cursor(&self, pos: &RowPtr) -> Cursor {
        self.sorted_buckets_view
            .get(&pos.bucket_ts)
            .expect("should exist")
            .row_pos(pos.row)
    }

    pub fn get_row_pos(&self, pos: &RowPtr) -> Cursor {
        self.get_cursor(pos)
    }

    pub fn get_row_args(&self, pos: &RowPtr) -> Vec<ArrayRef> {
        let view = self
            .sorted_buckets_view
            .get(&pos.bucket_ts)
            .expect("bucket view should exist");
        let args = view.args(self.window_id);
        args.iter().map(|a| a.slice(pos.row, 1)).collect()
    }

    pub fn has_next(&self, pos: &RowPtr) -> bool {
        if pos.bucket_ts > self.bucket_end_ts {
            return false;
        }
        let rows_in_bucket = self
            .sorted_buckets_view
            .get(&pos.bucket_ts)
            .expect("should exist")
            .size();
        if pos.bucket_ts < self.bucket_end_ts {
            return true;
        }
        pos.row + 1 < rows_in_bucket
    }

    pub fn next_pos(&self, pos: RowPtr) -> Option<RowPtr> {
        if !self.has_next(&pos) {
            return None;
        }
        Some(self.pos_n_rows(&pos, 1, false))
    }

    pub fn prev_pos(&self, pos: RowPtr) -> Option<RowPtr> {
        if pos.bucket_ts == self.bucket_start_ts && pos.row == 0 {
            return None;
        }
        Some(self.pos_n_rows(&pos, 1, true))
    }

    /// Get args for a range of positions (inclusive start, inclusive end).
    pub fn get_args_in_range(&self, start: &RowPtr, end: &RowPtr) -> Vec<ArrayRef> {
        use arrow::array::Array;

        if self.is_empty() {
            return vec![];
        }
        if start.bucket_ts > end.bucket_ts || (start.bucket_ts == end.bucket_ts && start.row > end.row) {
            return vec![];
        }

        let num_args = self
            .sorted_buckets_view
            .values()
            .next()
            .map(|v| v.args(self.window_id).len())
            .unwrap_or(0);
        if num_args == 0 {
            return vec![];
        }

        let mut per_arg_slices: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_args];

        let mut bucket_ts = start.bucket_ts;
        while bucket_ts <= end.bucket_ts {
            let view = self
                .sorted_buckets_view
                .get(&bucket_ts)
                .expect("bucket view should exist");
            let bucket_args = view.args(self.window_id);
            let bucket_rows = view.size();
            if bucket_rows == 0 {
                bucket_ts = self.bucket_granularity.next_start(bucket_ts);
                continue;
            }

            let (slice_start, slice_len) = if bucket_ts == start.bucket_ts && bucket_ts == end.bucket_ts {
                (start.row, end.row - start.row + 1)
            } else if bucket_ts == start.bucket_ts {
                (start.row, bucket_rows.saturating_sub(start.row))
            } else if bucket_ts == end.bucket_ts {
                (0, end.row + 1)
            } else {
                (0, bucket_rows)
            };

            if slice_len == 0 {
                continue;
            }

            for arg_idx in 0..num_args {
                per_arg_slices[arg_idx].push(bucket_args[arg_idx].slice(slice_start, slice_len));
            }

            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }

        let mut out: Vec<ArrayRef> = Vec::with_capacity(num_args);
        for slices in per_arg_slices {
            if slices.is_empty() {
                continue;
            }
            if slices.len() == 1 {
                out.push(slices[0].clone());
                continue;
            }
            let refs: Vec<&dyn Array> = slices.iter().map(|a| a.as_ref()).collect();
            let concatenated = arrow::compute::concat(&refs).expect("Should be able to concat arg slices");
            out.push(concatenated);
        }

        out
    }

    /// Count rows between positions (inclusive).
    pub fn count_between(&self, start: &RowPtr, end: &RowPtr) -> usize {
        if start.bucket_ts == end.bucket_ts {
            return end.row - start.row + 1;
        }
        let mut count = self
            .sorted_buckets_view
            .get(&start.bucket_ts)
            .expect("should exist")
            .size()
            .saturating_sub(start.row);

        let mut bucket_ts = self.bucket_granularity.next_start(start.bucket_ts);
        while bucket_ts < end.bucket_ts {
            count += self
                .sorted_buckets_view
                .get(&bucket_ts)
                .expect("should exist")
                .size();
            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }
        count += end.row + 1;
        count
    }

    /// Get position N rows before/after a given position.
    pub fn pos_n_rows(&self, pos: &RowPtr, n: usize, before: bool) -> RowPtr {
        if n == 0 {
            return *pos;
        }

        let mut current_bucket_ts = pos.bucket_ts;
        let mut current_row = pos.row;
        let mut remaining = n;

        while remaining > 0 && current_bucket_ts <= self.bucket_end_ts {
            let rows_in_current_bucket = self
                .sorted_buckets_view
                .get(&current_bucket_ts)
                .expect("should exist")
                .size();

            let (can_satisfy_in_bucket, available) = if before {
                (current_row >= remaining, current_row)
            } else {
                let available_in_bucket = rows_in_current_bucket - current_row;
                (available_in_bucket >= remaining, available_in_bucket)
            };

            if can_satisfy_in_bucket {
                if before {
                    current_row -= remaining;
                } else {
                    current_row += remaining;
                }
                remaining = 0;
            } else if before {
                remaining -= current_row + 1;
                if current_bucket_ts == self.bucket_start_ts {
                    current_row = 0;
                    break;
                }
                current_bucket_ts = self.bucket_granularity.prev_start(current_bucket_ts);
                current_row = self
                    .sorted_buckets_view
                    .get(&current_bucket_ts)
                    .expect("should exist")
                    .size()
                    .saturating_sub(1);
            } else {
                remaining -= available;
                let next_ts = self.bucket_granularity.next_start(current_bucket_ts);
                if next_ts <= self.bucket_end_ts {
                    current_bucket_ts = next_ts;
                    current_row = 0;
                } else {
                    current_bucket_ts = self.bucket_end_ts;
                    current_row = self
                        .sorted_buckets_view
                        .get(&current_bucket_ts)
                        .expect("should exist")
                        .size()
                        .saturating_sub(1);
                    break;
                }
            }
        }

        RowPtr::new(current_bucket_ts, current_row)
    }

    pub fn pos_from_end(&self, rows_from_end: usize) -> RowPtr {
        self.pos_n_rows(&self.last_pos(), rows_from_end, true)
    }

    fn seek_rowpos_in_bucket(&self, bucket_ts: Timestamp, target: Cursor, bound: SeekBound) -> Option<usize> {
        let view = self
            .sorted_buckets_view
            .get(&bucket_ts)
            .expect("bucket view should exist");
        let ts = view.timestamps();
        let seq = view.seq_nos();

        let mut lo: usize = 0;
        let mut hi: usize = ts.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mid_pos = Cursor::new(ts.value(mid), seq.value(mid));
            match bound {
                SeekBound::Ge => {
                    if mid_pos < target { lo = mid + 1 } else { hi = mid }
                }
                SeekBound::Gt => {
                    if mid_pos <= target { lo = mid + 1 } else { hi = mid }
                }
                SeekBound::Eq => {
                    if mid_pos < target { lo = mid + 1 } else { hi = mid }
                }
            }
        }
        if lo >= ts.len() {
            return None;
        }
        if matches!(bound, SeekBound::Eq) {
            let found = Cursor::new(ts.value(lo), seq.value(lo));
            if found == target { Some(lo) } else { None }
        } else {
            Some(lo)
        }
    }

    fn seek_rowpos(&self, target: Cursor, bound: SeekBound) -> Option<RowPtr> {
        let bucket_ts = self.bucket_granularity.start(target.ts);
        if bucket_ts < self.bucket_start_ts || bucket_ts > self.bucket_end_ts {
            return None;
        }
        match bound {
            SeekBound::Eq => self
                .seek_rowpos_in_bucket(bucket_ts, target, SeekBound::Eq)
                .map(|row| RowPtr::new(bucket_ts, row)),
            SeekBound::Ge | SeekBound::Gt => {
                let mut bucket_ts = bucket_ts;
                while bucket_ts <= self.bucket_end_ts {
                    if let Some(row) = self.seek_rowpos_in_bucket(bucket_ts, target, bound) {
                        return Some(RowPtr::new(bucket_ts, row));
                    }
                    bucket_ts = self.bucket_granularity.next_start(bucket_ts);
                }
                None
            }
        }
    }

    pub fn seek_rowpos_ge(&self, target: Cursor) -> Option<RowPtr> {
        self.seek_rowpos(target, SeekBound::Ge)
    }

    pub fn seek_rowpos_gt(&self, target: Cursor) -> Option<RowPtr> {
        self.seek_rowpos(target, SeekBound::Gt)
    }

    pub fn seek_rowpos_eq(&self, target: Cursor) -> Option<RowPtr> {
        self.seek_rowpos(target, SeekBound::Eq)
    }

    pub fn seek_ts_eq(&self, target_ts: Timestamp) -> Option<RowPtr> {
        let bucket_ts = self.bucket_granularity.start(target_ts);
        if bucket_ts < self.bucket_start_ts || bucket_ts > self.bucket_end_ts {
            return None;
        }
        let row = self.seek_rowpos_in_bucket(bucket_ts, Cursor::new(target_ts, 0), SeekBound::Ge)?;
        let p = RowPtr::new(bucket_ts, row);
        if self.get_timestamp(&p) == target_ts { Some(p) } else { None }
    }

    pub fn seek_ts_ge(&self, target_ts: Timestamp) -> Option<RowPtr> {
        self.seek_rowpos(Cursor::new(target_ts, 0), SeekBound::Ge)
    }

    pub fn seek_ts_gt(&self, target_ts: Timestamp) -> Option<RowPtr> {
        self.seek_rowpos(Cursor::new(target_ts, u64::MAX), SeekBound::Gt)
    }
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
    use crate::runtime::operators::window::Cursor;

    use super::{RowIndex, RowPtr};

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
    fn test_seek_ge_scans_forward_to_next_bucket() {
        let mut views = HashMap::new();
        views.insert(
            1000,
            make_view(1000, &[(1000, 1, 10), (1500, 2, 11)], 0),
        );
        views.insert(2000, make_view(2000, &[(2000, 1, 20)], 0));

        let idx = RowIndex::new(crate::runtime::operators::window::aggregates::BucketRange::new(1000, 2000), &views, 0, TimeGranularity::Seconds(1));

        // 1999 belongs to bucket 1000, but there's no row >= 1999 in that bucket.
        // GE should scan to the next bucket and return the first row there.
        let p = idx.seek_ts_ge(1999).expect("should find in next bucket");
        assert_eq!(p.bucket_ts, 2000);
        assert_eq!(p.row, 0);
        assert_eq!(idx.get_timestamp(&p), 2000);
    }

    #[test]
    fn test_seek_is_strict_out_of_range() {
        let mut views = HashMap::new();
        views.insert(2000, make_view(2000, &[(2000, 1, 20)], 0));

        let idx = RowIndex::new(crate::runtime::operators::window::aggregates::BucketRange::new(2000, 2000), &views, 0, TimeGranularity::Seconds(1));
        assert!(idx.seek_ts_ge(1000).is_none());
        assert!(idx.seek_rowpos_ge(Cursor::new(1000, 0)).is_none());
    }

    #[test]
    fn test_pos_n_rows_cross_bucket() {
        let mut views = HashMap::new();
        views.insert(1000, make_view(1000, &[(1000, 1, 10), (1500, 2, 11)], 0));
        views.insert(2000, make_view(2000, &[(2000, 1, 20), (2500, 2, 21)], 0));

        let idx = RowIndex::new(crate::runtime::operators::window::aggregates::BucketRange::new(1000, 2000), &views, 0, TimeGranularity::Seconds(1));

        let start = RowPtr::new(1000, 1); // last row in bucket 1000
        let p = idx.pos_n_rows(&start, 2, false);
        assert_eq!(p.bucket_ts, 2000);
        assert_eq!(p.row, 1);

        let back = idx.pos_n_rows(&p, 2, true);
        assert_eq!(back.bucket_ts, 1000);
        assert_eq!(back.row, 1);
    }

    #[test]
    fn test_get_args_in_range_concats_across_buckets() {
        let mut views = HashMap::new();
        views.insert(1000, make_view(1000, &[(1000, 1, 10), (1500, 2, 11)], 0));
        views.insert(2000, make_view(2000, &[(2000, 1, 20), (2500, 2, 21)], 0));

        let idx = RowIndex::new(crate::runtime::operators::window::aggregates::BucketRange::new(1000, 2000), &views, 0, TimeGranularity::Seconds(1));

        let start = RowPtr::new(1000, 1); // value 11
        let end = RowPtr::new(2000, 0); // value 20
        let args = idx.get_args_in_range(&start, &end);
        assert_eq!(args.len(), 1);

        let arr = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), 11);
        assert_eq!(arr.value(1), 20);
    }

    #[test]
    #[should_panic(expected = "missing bucket_ts=2000")]
    fn test_new_requires_contiguous_bucket_range() {
        let mut views = HashMap::new();
        // Missing bucket 2000 in [1000,3000] range
        views.insert(1000, make_view(1000, &[(1000, 1, 10)], 0));
        views.insert(3000, make_view(3000, &[(3000, 1, 30)], 0));

        let _idx = RowIndex::new(
            crate::runtime::operators::window::aggregates::BucketRange::new(1000, 3000),
            &views,
            0,
            TimeGranularity::Seconds(1),
        );
    }
}

