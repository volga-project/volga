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
///
/// This mirrors the old `RowIndex` API but operates directly over `SortedRangeView`
/// (no `SortedBucketView`, no `RowIndex`).
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
        let candidate = match after {
            Some(p) => self.prev_pos(p)?,
            None => self.last_pos_in_range()?,
        };
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
                    let pos = if bucket_ts == target_bucket {
                        self.seek_in_bucket(bucket_ts, cursor, SeekBound::Ge)?
                    } else {
                        RowPtr::new(bucket_ts, 0)
                    };
                    return (self.get_row_pos(&pos) <= self.end).then_some(pos);
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
                    let pos = if bucket_ts == target_bucket {
                        self.seek_in_bucket(bucket_ts, cursor, SeekBound::Gt)?
                    } else {
                        RowPtr::new(bucket_ts, 0)
                    };
                    return (self.get_row_pos(&pos) <= self.end).then_some(pos);
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
                self.prev_pos(cur).expect("pos_n_rows underflow")
            } else {
                self.next_pos(cur).expect("pos_n_rows overflow")
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
            pos = self.prev_pos(pos).expect("pos_from_end underflow");
            remaining -= 1;
        }
        pos
    }
}

