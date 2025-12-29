use std::collections::BTreeMap;

use datafusion::logical_expr::{WindowFrame, WindowFrameUnits};

use crate::runtime::operators::window::TimeGranularity;
use crate::runtime::operators::window::aggregates::BucketRange;
use crate::storage::batch_store::{BatchId, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor {
    pub ts: Timestamp,
    pub seq_no: u64,
}

impl Cursor {
    pub fn new(ts: Timestamp, seq_no: u64) -> Self {
        Self { ts, seq_no }
    }
}

/// Metadata stored per run/segment (immutable batch).
#[derive(Debug, Clone)]
pub struct RunMeta {
    pub batch_id: BatchId,
    pub bucket_timestamp: Timestamp,
    pub min_pos: Cursor,
    pub max_pos: Cursor,
    pub row_count: usize,
}

/// A bucket containing runs with the same bucket timestamp.
#[derive(Debug, Clone, Default)]
pub struct Bucket {
    pub timestamp: Timestamp,
    pub batches: Vec<RunMeta>,
    pub row_count: usize,
    pub version: u64,
}

impl Bucket {
    pub fn new(timestamp: Timestamp) -> Self {
        Self {
            timestamp,
            batches: Vec::new(),
            row_count: 0,
            version: 0,
        }
    }

    pub fn push(&mut self, metadata: RunMeta) {
        self.row_count += metadata.row_count;
        self.batches.push(metadata);
        self.version = self.version.wrapping_add(1);
    }
}

/// Result of sliding the window - returns buckets for aggregation layer.
#[derive(Debug, Clone)]
pub struct SlideRangeInfo {
    pub update_range: Option<BucketRange>,
    pub retract_range: Option<BucketRange>,
    pub new_processed_until: Cursor,
    /// Number of rows between retract's last bucket end and update's first bucket start.
    /// Used for ROWS window exact retract calculation at aggregation layer.
    pub row_distance: usize,
}

/// Bucket-level index using bucket timestamps.
#[derive(Debug, Clone)]
pub struct BucketIndex {
    buckets: BTreeMap<Timestamp, Bucket>, // TODO this can be hashmap for better performance
    total_rows: usize,
    max_pos_seen: Cursor,
    bucket_granularity: TimeGranularity,
}

impl BucketIndex {
    pub fn new(bucket_granularity: TimeGranularity) -> Self {
        Self {
            buckets: BTreeMap::new(),
            total_rows: 0,
            max_pos_seen: Cursor::new(i64::MIN, 0),
            bucket_granularity,
        }
    }

    pub fn bucket_granularity(&self) -> TimeGranularity {
        self.bucket_granularity
    }

    pub fn insert_batch(
        &mut self,
        batch_id: BatchId,
        min_pos: Cursor,
        max_pos: Cursor,
        row_count: usize,
    ) {
        let bucket_ts = batch_id.time_bucket() as Timestamp;

        let metadata = RunMeta {
            batch_id,
            bucket_timestamp: bucket_ts,
            min_pos,
            max_pos,
            row_count,
        };

        self.buckets
            .entry(bucket_ts)
            .or_insert_with(|| Bucket::new(bucket_ts))
            .push(metadata);
        self.total_rows += row_count;
        self.max_pos_seen = self.max_pos_seen.max(max_pos);
    }

    /// Query buckets with bucket_timestamp in range [start, end]
    pub fn query_buckets_in_range(&self, start: Timestamp, end: Timestamp) -> Vec<&Bucket> {
        self.buckets.range(start..=end).map(|(_, b)| b).collect()
    }

    pub fn plan_rows_tail(
        &self,
        end_ts: Timestamp,
        rows: usize,
        within: BucketRange,
    ) -> Option<BucketRange> {
        if rows == 0 || self.buckets.is_empty() {
            return None;
        }

        let end_bucket_ts = self.bucket_granularity.start(end_ts);
        let end_bucket_ts = end_bucket_ts.min(within.end);

        let buckets: Vec<&Bucket> = self
            .buckets
            .range(within.start..=end_bucket_ts)
            .map(|(_, b)| b)
            .collect();
        if buckets.is_empty() {
            return None;
        }

        // Walk backwards to cover `rows` by bucket row_count.
        let mut remaining = rows;
        let mut start_bucket_ts = buckets[0].timestamp;

        for b in buckets.iter().rev() {
            if remaining <= b.row_count {
                start_bucket_ts = b.timestamp;
                remaining = 0;
                break;
            }
            remaining -= b.row_count;
        }

        // If we didn't cover `rows`, we need everything within the range.
        if remaining > 0 {
            start_bucket_ts = buckets[0].timestamp;
        }

        Some(BucketRange::new(start_bucket_ts, end_bucket_ts))
    }

    /// Get buckets for RANGE windows given update bucket timestamps.
    /// For update range `[start,end]`, returns a bucket range covering
    /// `[start - window_length_ms, end]` (clamped to existing buckets).
    pub fn get_relevant_range_for_range_windows(
        &self,
        update_range: BucketRange,
        window_length_ms: i64,
    ) -> BucketRange {
        if self.buckets.is_empty() {
            return update_range;
        }
        let start_unaligned = update_range.start.saturating_sub(window_length_ms);
        let start = self.bucket_granularity.start(start_unaligned);
        let end = update_range.end;

        let mut iter = self.buckets.range(start..=end);
        let first = iter.next().map(|(k, _)| *k);
        let last = self.buckets.range(start..=end).next_back().map(|(k, _)| *k);
        match (first, last) {
            (Some(s), Some(e)) => BucketRange::new(s, e),
            _ => update_range,
        }
    }

    /// Get buckets for ROWS windows given update bucket timestamps.
    /// Returns a bucket range that contains `[start,end]` and additionally walks backwards from `start`
    /// collecting at least `window_size` rows (clamped to existing buckets).
    pub fn get_relevant_range_for_rows_windows(
        &self,
        update_range: BucketRange,
        window_size: usize,
    ) -> BucketRange {
        if self.buckets.is_empty() {
            return update_range;
        }

        // Include all buckets in [start,end]
        let mut rows_in_range: usize = 0;
        for (_, bucket) in self.buckets.range(update_range.start..=update_range.end) {
            rows_in_range += bucket.row_count;
        }

        let mut start = update_range.start;
        if rows_in_range < window_size {
            let mut rows_needed = window_size - rows_in_range;
            for (&bucket_ts, bucket) in self.buckets.range(..update_range.start).rev() {
                start = bucket_ts;
                if rows_needed <= bucket.row_count {
                    break;
                }
                rows_needed -= bucket.row_count;
            }
        }

        // Clamp start/end to existing buckets (defensive).
        let end = update_range.end;
        let first = self.buckets.range(start..=end).next().map(|(k, _)| *k);
        let last = self.buckets.range(start..=end).next_back().map(|(k, _)| *k);
        match (first, last) {
            (Some(s), Some(e)) => BucketRange::new(s, e),
            _ => update_range,
        }
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn max_pos_seen(&self) -> Cursor {
        self.max_pos_seen
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    pub fn bucket_timestamps(&self) -> Vec<Timestamp> {
        self.buckets.keys().copied().collect()
    }

    pub fn prune(&mut self, cutoff_timestamp: Timestamp) -> Vec<BatchId> {
        let mut pruned_ids = Vec::new();
        let mut empty_buckets = Vec::new();

        for (&bucket_ts, bucket) in self.buckets.iter_mut() {
            let before_len = bucket.batches.len();
            bucket.batches.retain(|b| {
                if b.max_pos.ts < cutoff_timestamp {
                    pruned_ids.push(b.batch_id);
                    self.total_rows -= b.row_count;
                    bucket.row_count -= b.row_count;
                    false
                } else {
                    true
                }
            });
            if bucket.batches.len() != before_len {
                bucket.version = bucket.version.wrapping_add(1);
            }
            if bucket.batches.is_empty() {
                empty_buckets.push(bucket_ts);
            }
        }

        for bucket_ts in empty_buckets {
            self.buckets.remove(&bucket_ts);
        }

        pruned_ids
    }

    /// Slide window for retractable aggregations.
    pub fn slide_window(
        &self,
        window_frame: &WindowFrame,
        previous_processed_until: Option<Cursor>,
    ) -> Option<SlideRangeInfo> {
        self.slide_window_until(window_frame, previous_processed_until, self.max_pos_seen)
    }

    /// Slide window but cap progress at `until` (typically watermark).
    ///
    /// Note: if `until` is ahead of what we have ingested (`max_pos_seen`), we clamp to `max_pos_seen`.
    pub fn slide_window_until(
        &self,
        window_frame: &WindowFrame,
        previous_processed_until: Option<Cursor>,
        until: Cursor,
    ) -> Option<SlideRangeInfo> {
        if self.buckets.is_empty() {
            return None;
        }

        let prev = previous_processed_until.unwrap_or(Cursor::new(i64::MIN, 0));
        let until = until.min(self.max_pos_seen);

        match window_frame.units {
            WindowFrameUnits::Range => {
                let window_length_ms = get_window_length_ms(window_frame);
                Some(self.slide_range_window(window_length_ms, prev, until))
            }
            WindowFrameUnits::Rows => {
                let window_size = get_window_size_rows(window_frame);
                Some(self.slide_rows_window(window_size, prev, until))
            }
            WindowFrameUnits::Groups => panic!("Groups window frame not supported"),
        }
    }

    fn slide_range_window(&self, window_length_ms: i64, prev: Cursor, new_end: Cursor) -> SlideRangeInfo {

        if new_end <= prev {
            return SlideRangeInfo {
                update_range: None,
                retract_range: None,
                new_processed_until: prev,
                row_distance: 0,
            };
        }

        let new_bucket_ts = self.bucket_granularity.start(new_end.ts);
        let update_buckets: Vec<&Bucket> = if prev.ts == i64::MIN {
            self.buckets.range(..=new_bucket_ts).map(|(_, b)| b).collect()
        } else {
            let prev_bucket_ts = self.bucket_granularity.start(prev.ts);
            self.buckets
                .range(prev_bucket_ts..=new_bucket_ts)
                .map(|(_, b)| b)
                .collect()
        };

        if update_buckets.is_empty() {
            return SlideRangeInfo {
                update_range: None,
                retract_range: None,
                new_processed_until: prev,
                row_distance: 0,
            };
        }

        let prev_start_ts = prev.ts.saturating_sub(window_length_ms);
        let new_start_ts = new_end.ts.saturating_sub(window_length_ms);

        let retract_buckets = if new_start_ts > prev_start_ts && prev.ts > i64::MIN {
            self.buckets
                .range(prev_start_ts..new_start_ts)
                .map(|(_, b)| b)
                .collect()
        } else {
            vec![]
        };

        let update_range = Some(BucketRange::new(
            update_buckets.first().unwrap().timestamp,
            update_buckets.last().unwrap().timestamp,
        ));

        let retract_range = if prev.ts == i64::MIN {
            // First run: retract happens within the update stream itself.
            update_range
        } else if retract_buckets.is_empty() {
            None
        } else {
            Some(BucketRange::new(
                retract_buckets.first().unwrap().timestamp,
                retract_buckets.last().unwrap().timestamp,
            ))
        };

        SlideRangeInfo {
            update_range,
            retract_range,
            new_processed_until: new_end,
            row_distance: 0,
        }
    }

    fn slide_rows_window(&self, window_size: usize, prev: Cursor, new_end: Cursor) -> SlideRangeInfo {

        if new_end <= prev {
            return SlideRangeInfo {
                update_range: None,
                retract_range: None,
                new_processed_until: prev,
                row_distance: 0,
            };
        }

        let new_bucket_ts = self.bucket_granularity.start(new_end.ts);
        let update_buckets: Vec<&Bucket> = if prev.ts == i64::MIN {
            self.buckets.range(..=new_bucket_ts).map(|(_, b)| b).collect()
        } else {
            let prev_bucket_ts = self.bucket_granularity.start(prev.ts);
            self.buckets
                .range(prev_bucket_ts..=new_bucket_ts)
                .map(|(_, b)| b)
                .collect()
        };

        if update_buckets.is_empty() {
            return SlideRangeInfo {
                update_range: None,
                retract_range: None,
                new_processed_until: prev,
                row_distance: 0,
            };
        }

        let (retract_buckets, row_distance) = if prev.ts > i64::MIN && self.total_rows > window_size {
            let new_start_bucket_ts = update_buckets.first().unwrap().timestamp;

            let mut rows_counted = 0;
            let mut old_end_bucket_ts: Option<Timestamp> = None;
            let mut old_start_bucket_ts: Option<Timestamp> = None;

            for (&bucket_ts, bucket) in self.buckets.range(..new_start_bucket_ts).rev() {
                if old_end_bucket_ts.is_none() {
                    old_end_bucket_ts = Some(bucket_ts);
                }
                old_start_bucket_ts = Some(bucket_ts);
                rows_counted += bucket.row_count;
                if rows_counted >= window_size {
                    break;
                }
            }

            match (old_start_bucket_ts, old_end_bucket_ts) {
                (Some(old_start), Some(old_end)) => {
                    let retract_buckets: Vec<&Bucket> = self.buckets
                        .range(old_start..=old_end)
                        .map(|(_, b)| b)
                        .collect();

                    let row_distance: usize = self.buckets
                        .range((old_end + 1)..new_start_bucket_ts)
                        .map(|(_, b)| b.row_count)
                        .sum();

                    (retract_buckets, row_distance)
                }
                _ => (vec![], 0),
            }
        } else {
            (vec![], 0)
        };

        let update_range = Some(BucketRange::new(
            update_buckets.first().unwrap().timestamp,
            update_buckets.last().unwrap().timestamp,
        ));

        let retract_range = if prev.ts == i64::MIN {
            // First run: retract happens within the update stream itself.
            update_range
        } else if retract_buckets.is_empty() {
            None
        } else {
            Some(BucketRange::new(
                retract_buckets.first().unwrap().timestamp,
                retract_buckets.last().unwrap().timestamp,
                ))
        };

        SlideRangeInfo {
            update_range,
            retract_range,
            new_processed_until: new_end,
            row_distance,
        }
    }
}

pub fn get_window_length_ms(window_frame: &WindowFrame) -> i64 {
    use datafusion::logical_expr::WindowFrameBound;

    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => match value {
            datafusion::scalar::ScalarValue::IntervalMonthDayNano(Some(v)) => {
                (v.nanoseconds / 1_000_000) + (v.days as i64 * 24 * 60 * 60 * 1000)
            }
            datafusion::scalar::ScalarValue::UInt64(Some(v)) => *v as i64,
            datafusion::scalar::ScalarValue::Int64(Some(v)) => *v,
            _ => panic!("Unsupported window frame bound type: {:?}", value),
        },
        _ => panic!(
            "Unsupported window frame start bound: {:?}",
            window_frame.start_bound
        ),
    }
}

pub fn get_window_size_rows(window_frame: &WindowFrame) -> usize {
    use datafusion::logical_expr::WindowFrameBound;

    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => match value {
            datafusion::scalar::ScalarValue::UInt64(Some(v)) => (*v as usize) + 1,
            datafusion::scalar::ScalarValue::Int64(Some(v)) => (*v as usize) + 1,
            _ => panic!("Unsupported ROWS window frame bound type: {:?}", value),
        },
        _ => panic!(
            "Unsupported window frame start bound: {:?}",
            window_frame.start_bound
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch_id(time_bucket: Timestamp, uid: u64) -> BatchId {
        BatchId::new(123, time_bucket, uid)
    }

    fn make_pos(ts: Timestamp, seq: u64) -> Cursor {
        Cursor::new(ts, seq)
    }

    fn assert_range(r: BucketRange, start: Timestamp, end: Timestamp) {
        assert_eq!(r.start, start);
        assert_eq!(r.end, end);
    }

    #[test]
    fn test_insert_and_query() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 150), 50);
        index.insert_batch(make_batch_id(3000, 3), make_pos(3000, 151), make_pos(3500, 225), 75);

        assert_eq!(index.total_rows(), 225);
        assert_eq!(index.max_pos_seen().ts, 3500);

        let buckets = index.query_buckets_in_range(1500, 2500);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].timestamp, 2000); // only bucket 2000

        let buckets = index.query_buckets_in_range(1000, 3000);
        assert_eq!(buckets.len(), 3);
    }

    #[test]
    fn test_insert_same_bucket() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(1000, 2), make_pos(1100, 101), make_pos(1600, 150), 50);

        let buckets = index.query_buckets_in_range(1000, 1000);
        assert_eq!(buckets.len(), 1);
        let bucket = buckets[0];
        assert_eq!(bucket.row_count, 150);
        assert_eq!(bucket.batches.len(), 2);
    }

    #[test]
    fn test_slide_range_window() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));
        let window_length = 1000;

        // Step 1: initial slide on empty index
        let result = index.slide_range_window(window_length, Cursor::new(i64::MIN, 0), index.max_pos_seen());
        assert!(result.update_range.is_none());
        assert!(result.retract_range.is_none());
        let mut prev_end = result.new_processed_until;

        // Step 2: add data, slide
        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 150), 50);

        let result = index.slide_range_window(window_length, prev_end, index.max_pos_seen());
        let update_r = result.update_range.unwrap();
        assert_range(update_r, 1000, 2000);
        assert!(result.retract_range.is_none());
        assert_eq!(result.new_processed_until.ts, 2500);
        prev_end = result.new_processed_until;

        // Step 3: add more data (including out-of-order), slide
        index.insert_batch(make_batch_id(4000, 3), make_pos(4000, 151), make_pos(4500, 225), 75);
        index.insert_batch(make_batch_id(1500, 4), make_pos(1500, 226), make_pos(1800, 255), 30); // out-of-order
        index.insert_batch(make_batch_id(3000, 5), make_pos(3000, 256), make_pos(3500, 315), 60);

        let result = index.slide_range_window(window_length, prev_end, index.max_pos_seen());
        let update_r = result.update_range.unwrap();
        assert_range(update_r, 2000, 4000);

        let retract_r = result.retract_range.unwrap();
        assert_range(retract_r, 1500, 3000);

        assert_eq!(result.new_processed_until.ts, 4500);
    }

    #[test]
    fn test_slide_rows_window() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));
        let window_size = 150;

        // Step 1: initial slide on empty index
        let result = index.slide_rows_window(window_size, Cursor::new(i64::MIN, 0), index.max_pos_seen());
        assert!(result.update_range.is_none());
        assert!(result.retract_range.is_none());
        let mut prev_end = result.new_processed_until;

        // Step 2: add data, slide (200 rows total, window=150, no retracts yet)
        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 200), 100);

        let result = index.slide_rows_window(window_size, prev_end, index.max_pos_seen());
        let update_r = result.update_range.unwrap();
        assert_range(update_r, 1000, 2000);
        assert!(result.retract_range.is_none());
        assert_eq!(result.new_processed_until.ts, 2500);
        prev_end = result.new_processed_until;

        // Step 3: add more data (including out-of-order)
        index.insert_batch(make_batch_id(4000, 3), make_pos(4000, 201), make_pos(4500, 300), 100);
        index.insert_batch(make_batch_id(1500, 4), make_pos(1500, 301), make_pos(1800, 350), 50); // out-of-order
        index.insert_batch(make_batch_id(3000, 5), make_pos(3000, 351), make_pos(3500, 450), 100);

        let result = index.slide_rows_window(window_size, prev_end, index.max_pos_seen());
        let update_r = result.update_range.unwrap();
        assert_range(update_r, 2000, 4000);

        let retract_r = result.retract_range.unwrap();
        assert_range(retract_r, 1000, 1500);

        assert_eq!(result.new_processed_until.ts, 4500);
    }

    #[test]
    fn test_get_relevant_buckets_for_range_windows() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 150), 50);
        index.insert_batch(make_batch_id(3000, 3), make_pos(3000, 151), make_pos(3500, 225), 75);
        index.insert_batch(make_batch_id(5000, 4), make_pos(5000, 226), make_pos(5500, 285), 60);

        // Update at 3000, window=1000 -> [2000, 3000]
        let update = BucketRange::new(3000, 3000);
        let r = index.get_relevant_range_for_range_windows(update, 1000);
        assert_range(r, 2000, 3000);

        // Update at 5000, window=1000 -> [4000, 5000] (only 5000 exists)
        let update = BucketRange::new(5000, 5000);
        let r = index.get_relevant_range_for_range_windows(update, 1000);
        assert_range(r, 5000, 5000);
    }

    #[test]
    fn test_get_relevant_buckets_for_rows_windows() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 200), 100);
        index.insert_batch(make_batch_id(3000, 3), make_pos(3000, 201), make_pos(3500, 300), 100);
        index.insert_batch(make_batch_id(4000, 4), make_pos(4000, 301), make_pos(4500, 400), 100);

        // window=50 -> only bucket 4000 (100 rows > 50)
        let update = BucketRange::new(4000, 4000);
        let r = index.get_relevant_range_for_rows_windows(update, 50);
        assert_range(r, 4000, 4000);

        // window=150 -> buckets 3000 + 4000
        let update = BucketRange::new(4000, 4000);
        let r = index.get_relevant_range_for_rows_windows(update, 150);
        assert_range(r, 3000, 4000);

        // window=350 -> all
        let update = BucketRange::new(4000, 4000);
        let r = index.get_relevant_range_for_rows_windows(update, 350);
        assert_range(r, 1000, 4000);

        // window bigger than all data -> all
        let update = BucketRange::new(4000, 4000);
        let r = index.get_relevant_range_for_rows_windows(update, 1000);
        assert_range(r, 1000, 4000);
    }

    #[test]
    fn test_prune() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 150), 50);
        index.insert_batch(make_batch_id(3000, 3), make_pos(3000, 151), make_pos(3500, 225), 75);

        let pruned = index.prune(2000);
        assert_eq!(pruned.len(), 1);
        assert_eq!(index.total_rows(), 125);
        assert_eq!(index.bucket_timestamps(), vec![2000, 3000]);
    }
}


