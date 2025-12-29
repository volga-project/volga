use std::cmp::Ordering;

use arrow::array::{Array, ArrayRef};

use crate::runtime::operators::window::aggregates::BucketRange;
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::{Cursor, RowPtr};
use crate::storage::batch_store::Timestamp;

use super::sorted_range_view::{SortedRangeBucket, SortedRangeView};

#[derive(Clone, Copy, Debug)]
enum SeekBound {
    Ge,
    Gt,
    Eq,
}

/// A lightweight index over a single `SortedRangeView`.
pub struct SortedRangeIndex<'a> {
    bucket_range: BucketRange,
    start: Cursor,
    end: Cursor,
    bucket_granularity: TimeGranularity,
    buckets: &'a std::collections::HashMap<Timestamp, SortedRangeBucket>,
}

impl<'a> SortedRangeIndex<'a> {
    pub fn new(view: &'a SortedRangeView) -> Self {
        Self {
            bucket_range: view.bucket_range(),
            start: view.start(),
            end: view.end(),
            bucket_granularity: view.bucket_granularity(),
            buckets: view.buckets(),
        }
    }

    pub fn new_in_bucket_range(view: &'a SortedRangeView, bucket_range: BucketRange) -> Self {
        Self {
            bucket_range,
            start: view.start(),
            end: view.end(),
            bucket_granularity: view.bucket_granularity(),
            buckets: view.buckets(),
        }
    }

    fn bucket_opt(&self, bucket_ts: Timestamp) -> Option<&SortedRangeBucket> {
        self.buckets.get(&bucket_ts)
    }

    pub fn bucket_size(&self, bucket_ts: Timestamp) -> usize {
        self.bucket_opt(bucket_ts).map(|b| b.size()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.first_pos_opt().is_none()
    }

    pub fn first_pos(&self) -> RowPtr {
        self.first_pos_opt().expect("SortedRangeIndex is empty")
    }

    pub fn last_pos(&self) -> RowPtr {
        self.last_pos_opt().expect("SortedRangeIndex is empty")
    }

    fn first_pos_opt(&self) -> Option<RowPtr> {
        self.seek_rowpos_ge(self.start)
    }

    fn last_pos_opt(&self) -> Option<RowPtr> {
        let after = self.seek_rowpos_gt(self.end);
        let mut candidate = match after {
            Some(p) => self.prev_pos(p)?,
            None => self.last_pos_in_range()?,
        };

        // Clamp to `end` (we want the last row with Cursor <= end).
        while self.get_row_pos(&candidate) > self.end {
            candidate = self.prev_pos(candidate)?;
        }

        (self.get_row_pos(&candidate) >= self.start).then_some(candidate)
    }

    fn last_pos_in_range(&self) -> Option<RowPtr> {
        let mut bucket_ts = self.bucket_range.end;
        loop {
            if let Some(b) = self.bucket_opt(bucket_ts) {
                if b.size() > 0 {
                    return Some(RowPtr::new(bucket_ts, b.size() - 1));
                }
            }
            if bucket_ts == self.bucket_range.start {
                return None;
            }
            bucket_ts = self.bucket_granularity.prev_start(bucket_ts);
        }
    }

    pub fn get_timestamp(&self, pos: &RowPtr) -> Timestamp {
        self.bucket_opt(pos.bucket_ts)
            .expect("bucket must exist")
            .timestamp(pos.row)
    }

    pub fn get_cursor(&self, pos: &RowPtr) -> Cursor {
        self.bucket_opt(pos.bucket_ts)
            .expect("bucket must exist")
            .row_pos(pos.row)
    }

    pub fn get_row_pos(&self, pos: &RowPtr) -> Cursor {
        self.get_cursor(pos)
    }

    pub fn get_row_args(&self, pos: &RowPtr) -> Vec<ArrayRef> {
        let b = self.bucket_opt(pos.bucket_ts).expect("bucket must exist");
        b.args().iter().map(|a| a.slice(pos.row, 1)).collect()
    }

    pub fn get_args_in_range(&self, start: &RowPtr, end: &RowPtr) -> Vec<ArrayRef> {
        if start.bucket_ts > end.bucket_ts || (start.bucket_ts == end.bucket_ts && start.row > end.row) {
            return vec![];
        }

        let num_args = self
            .buckets
            .values()
            .next()
            .map(|b| b.args().len())
            .unwrap_or(0);
        if num_args == 0 {
            return vec![];
        }

        let mut per_arg_slices: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_args];

        let mut bucket_ts = start.bucket_ts;
        while bucket_ts <= end.bucket_ts {
            let Some(bucket) = self.bucket_opt(bucket_ts) else {
                bucket_ts = self.bucket_granularity.next_start(bucket_ts);
                continue;
            };
            let bucket_rows = bucket.size();
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
                bucket_ts = self.bucket_granularity.next_start(bucket_ts);
                continue;
            }

            let args = bucket.args();
            for arg_idx in 0..num_args {
                per_arg_slices[arg_idx].push(args[arg_idx].slice(slice_start, slice_len));
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
            let concatenated = arrow::compute::concat(&refs).expect("concat arg slices");
            out.push(concatenated);
        }
        out
    }

    fn seek_in_bucket(&self, bucket_ts: Timestamp, cursor: Cursor, bound: SeekBound) -> Option<RowPtr> {
        let bucket = self.bucket_opt(bucket_ts)?;
        let n = bucket.size();
        if n == 0 {
            return None;
        }

        let ts_arr = bucket.timestamps();
        let seq_arr = bucket.seq_nos();

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mid_cursor = Cursor::new(ts_arr.value(mid), seq_arr.value(mid));
            let cmp = mid_cursor.cmp(&cursor);
            match bound {
                SeekBound::Ge => {
                    if cmp == Ordering::Less {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                SeekBound::Gt => {
                    if cmp == Ordering::Greater {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                SeekBound::Eq => {
                    if cmp == Ordering::Less {
                        lo = mid + 1;
                    } else if cmp == Ordering::Greater {
                        hi = mid;
                    } else {
                        return Some(RowPtr::new(bucket_ts, mid));
                    }
                }
            }
        }

        match bound {
            SeekBound::Eq => None,
            _ => (lo < n).then_some(RowPtr::new(bucket_ts, lo)),
        }
    }

    pub fn seek_rowpos_eq(&self, cursor: Cursor) -> Option<RowPtr> {
        if cursor < self.start || cursor > self.end {
            return None;
        }
        let bucket_ts = self.bucket_granularity.start(cursor.ts);
        if bucket_ts < self.bucket_range.start || bucket_ts > self.bucket_range.end {
            return None;
        }
        self.seek_in_bucket(bucket_ts, cursor, SeekBound::Eq)
    }

    pub fn seek_rowpos_ge(&self, cursor: Cursor) -> Option<RowPtr> {
        let cursor = cursor.max(self.start);
        let target_bucket = self.bucket_granularity.start(cursor.ts);
        let mut bucket_ts = target_bucket.max(self.bucket_range.start);
        while bucket_ts <= self.bucket_range.end {
            if let Some(b) = self.bucket_opt(bucket_ts) {
                if b.size() > 0 {
                    let pos_opt = if bucket_ts == target_bucket {
                        self.seek_in_bucket(bucket_ts, cursor, SeekBound::Ge)
                    } else {
                        Some(RowPtr::new(bucket_ts, 0))
                    };
                    if let Some(pos) = pos_opt {
                        return (self.get_row_pos(&pos) <= self.end).then_some(pos);
                    }
                    // No row >= cursor in this bucket; continue to next bucket.
                }
            }
            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }
        None
    }

    pub fn seek_rowpos_gt(&self, cursor: Cursor) -> Option<RowPtr> {
        if cursor >= self.end {
            return None;
        }
        let cursor = cursor.max(self.start);
        let target_bucket = self.bucket_granularity.start(cursor.ts);
        let mut bucket_ts = target_bucket.max(self.bucket_range.start);
        while bucket_ts <= self.bucket_range.end {
            if let Some(b) = self.bucket_opt(bucket_ts) {
                if b.size() > 0 {
                    let pos_opt = if bucket_ts == target_bucket {
                        self.seek_in_bucket(bucket_ts, cursor, SeekBound::Gt)
                    } else {
                        Some(RowPtr::new(bucket_ts, 0))
                    };
                    if let Some(pos) = pos_opt {
                        return (self.get_row_pos(&pos) <= self.end).then_some(pos);
                    }
                    // No row > cursor in this bucket; continue to next bucket.
                }
            }
            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }
        None
    }

    pub fn seek_ts_ge(&self, ts: Timestamp) -> Option<RowPtr> {
        self.seek_rowpos_ge(Cursor::new(ts, 0))
    }

    pub fn seek_ts_gt(&self, ts: Timestamp) -> Option<RowPtr> {
        self.seek_rowpos_gt(Cursor::new(ts, u64::MAX))
    }

    pub fn next_pos(&self, pos: RowPtr) -> Option<RowPtr> {
        let b = self.bucket_opt(pos.bucket_ts)?;
        if pos.row + 1 < b.size() {
            let next = RowPtr::new(pos.bucket_ts, pos.row + 1);
            return (self.get_row_pos(&next) <= self.end).then_some(next);
        }

        let mut bucket_ts = self.bucket_granularity.next_start(pos.bucket_ts);
        while bucket_ts <= self.bucket_range.end {
            if let Some(b) = self.bucket_opt(bucket_ts) {
                if b.size() > 0 {
                    let next = RowPtr::new(bucket_ts, 0);
                    return (self.get_row_pos(&next) <= self.end).then_some(next);
                }
            }
            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }
        None
    }

    pub fn prev_pos(&self, pos: RowPtr) -> Option<RowPtr> {
        if pos.bucket_ts == self.bucket_range.start && pos.row == 0 {
            return None;
        }
        if pos.row > 0 {
            let prev = RowPtr::new(pos.bucket_ts, pos.row - 1);
            return (self.get_row_pos(&prev) >= self.start).then_some(prev);
        }

        let mut bucket_ts = pos.bucket_ts;
        loop {
            if bucket_ts == self.bucket_range.start {
                return None;
            }
            bucket_ts = self.bucket_granularity.prev_start(bucket_ts);
            if let Some(b) = self.bucket_opt(bucket_ts) {
                if b.size() > 0 {
                    let prev = RowPtr::new(bucket_ts, b.size() - 1);
                    return (self.get_row_pos(&prev) >= self.start).then_some(prev);
                }
            }
        }
    }

    pub fn count_between(&self, start: &RowPtr, end: &RowPtr) -> usize {
        if start.bucket_ts == end.bucket_ts {
            return end.row - start.row + 1;
        }

        let mut count = self
            .bucket_opt(start.bucket_ts)
            .expect("bucket must exist")
            .size()
            .saturating_sub(start.row);

        let mut bucket_ts = self.bucket_granularity.next_start(start.bucket_ts);
        while bucket_ts < end.bucket_ts {
            if let Some(b) = self.bucket_opt(bucket_ts) {
                count += b.size();
            }
            bucket_ts = self.bucket_granularity.next_start(bucket_ts);
        }
        count += end.row + 1;
        count
    }

    pub fn pos_n_rows(&self, pos: &RowPtr, n: usize, back: bool) -> RowPtr {
        if n == 0 {
            return *pos;
        }
        let mut cur = *pos;
        let mut remaining = n;
        while remaining > 0 {
            cur = if back {
                match self.prev_pos(cur) {
                    Some(p) => p,
                    None => return self.first_pos(),
                }
            } else {
                match self.next_pos(cur) {
                    Some(p) => p,
                    None => return self.last_pos(),
                }
            };
            remaining -= 1;
        }
        cur
    }

    /// Return the position `offset` rows from the end (0 => last row).
    pub fn pos_from_end(&self, offset: usize) -> RowPtr {
        let mut pos = self.last_pos();
        let mut remaining = offset;
        while remaining > 0 {
            match self.prev_pos(pos) {
                Some(p) => pos = p,
                None => return self.first_pos(),
            }
            remaining -= 1;
        }
        pos
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use crate::runtime::operators::window::aggregates::BucketRange;
    use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeBucket, SortedRangeView};
    use crate::runtime::operators::window::tiles::TimeGranularity;
    use crate::runtime::operators::window::Cursor;
    use crate::runtime::operators::window::RowPtr;
    use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;

    use super::SortedRangeIndex;

    fn batch_ts_seq(rows: &[(i64, u64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
            Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false),
        ]));

        let ts = Arc::new(TimestampMillisecondArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let v = Arc::new(Float64Array::from(vec![0.0; rows.len()]));
        let pk = Arc::new(StringArray::from(vec!["A"; rows.len()]));
        let seq = Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(schema, vec![ts, v, pk, seq]).expect("test batch")
    }

    fn view(
        gran: TimeGranularity,
        bucket_range: BucketRange,
        start: Cursor,
        end: Cursor,
        buckets: Vec<(i64, Vec<(i64, u64)>)>,
    ) -> SortedRangeView {
        let request = DataRequest {
            bucket_range,
            bounds: DataBounds::All,
        };
        let mut map: HashMap<i64, SortedRangeBucket> = HashMap::new();
        for (bucket_ts, rows) in buckets {
            let b = batch_ts_seq(&rows);
            map.insert(bucket_ts, SortedRangeBucket::new(bucket_ts, b, 0, 3, Arc::new(vec![])));
        }
        SortedRangeView::new(request, gran, start, end, map)
    }

    #[test]
    fn seek_rowpos_ge_scans_forward_across_buckets() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 3000),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            vec![
                (1000, vec![]),
                (2000, vec![(2000, 0)]),
                (3000, vec![(3000, 0)]),
            ],
        );
        let idx = SortedRangeIndex::new(&v);

        let p = idx.seek_rowpos_ge(Cursor::new(1500, 0)).expect("should find");
        assert_eq!(p, RowPtr::new(2000, 0));
    }

    #[test]
    fn seek_rowpos_eq_is_strict_to_target_bucket() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 3000),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            vec![(1000, vec![(1000, 0)]), (2000, vec![(2000, 0)]), (3000, vec![(3000, 0)])],
        );
        let idx = SortedRangeIndex::new(&v);

        // Cursor maps to bucket 1000, but isn't present there.
        assert!(idx.seek_rowpos_eq(Cursor::new(1500, 0)).is_none());
    }

    #[test]
    fn next_prev_cross_bucket_boundaries() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 3000),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            vec![
                (1000, vec![(1000, 0), (1100, 1)]),
                (2000, vec![(2000, 2)]),
                (3000, vec![]),
            ],
        );
        let idx = SortedRangeIndex::new(&v);

        let last_in_1000 = RowPtr::new(1000, 1);
        assert_eq!(idx.next_pos(last_in_1000), Some(RowPtr::new(2000, 0)));
        assert_eq!(idx.prev_pos(RowPtr::new(2000, 0)), Some(last_in_1000));
    }

    #[test]
    fn pos_n_rows_clamps_instead_of_panicking() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 1000),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            vec![(1000, vec![(1000, 0), (1100, 1)])],
        );
        let idx = SortedRangeIndex::new(&v);

        let first = idx.first_pos();
        let last = idx.last_pos();
        assert_eq!(idx.pos_n_rows(&first, 10, true), first);
        assert_eq!(idx.pos_n_rows(&last, 10, false), last);
    }

    #[test]
    fn pos_from_end_clamps_instead_of_panicking() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 1000),
            Cursor::new(i64::MIN, 0),
            Cursor::new(i64::MAX, u64::MAX),
            vec![(1000, vec![(1000, 0), (1100, 1)])],
        );
        let idx = SortedRangeIndex::new(&v);

        assert_eq!(idx.pos_from_end(0), idx.last_pos());
        assert_eq!(idx.pos_from_end(999), idx.first_pos());
    }

    #[test]
    fn first_last_respect_cursor_bounds() {
        let gran = TimeGranularity::Seconds(1);
        let v = view(
            gran,
            BucketRange::new(1000, 3000),
            Cursor::new(1500, 0),
            Cursor::new(2500, u64::MAX),
            vec![
                (1000, vec![(1000, 0)]),
                (2000, vec![(2000, 0)]),
                (3000, vec![(3000, 0)]),
            ],
        );
        let idx = SortedRangeIndex::new(&v);

        assert_eq!(idx.first_pos(), RowPtr::new(2000, 0));
        assert_eq!(idx.last_pos(), RowPtr::new(2000, 0));
    }
}

