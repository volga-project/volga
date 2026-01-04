use std::collections::BTreeMap;

use datafusion::logical_expr::WindowFrame;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::TimeGranularity;
use crate::runtime::operators::window::aggregates::BucketRange;
use crate::storage::batch_store::{BatchId, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cursor {
    pub ts: Timestamp,
    pub seq_no: u64,
}

impl Cursor {
    pub fn new(ts: Timestamp, seq_no: u64) -> Self {
        Self { ts, seq_no }
    }
}

/// Opaque identifier for an in-memory (hot) batch.
///
/// This is intentionally *not* a `BatchId` and is store-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InMemBatchId(pub u64);

/// A reference to an immutable sorted batch/segment.
///
/// Batches can be either:
/// - `InMem`: hot in-memory batches, referenced by an opaque id resolved by the window state
/// - `Stored`: persisted batches, referenced by the storage backend id (currently `BatchId`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchRef {
    InMem(InMemBatchId),
    Stored(BatchId),
}

impl BatchRef {
    pub fn stored_batch_id(&self) -> Option<BatchId> {
        match self {
            BatchRef::Stored(id) => Some(*id),
            BatchRef::InMem(_) => None,
        }
    }

    pub fn inmem_id(&self) -> Option<InMemBatchId> {
        match self {
            BatchRef::InMem(id) => Some(*id),
            BatchRef::Stored(_) => None,
        }
    }
}

/// Metadata stored per batch/segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMeta {
    pub run: BatchRef,
    pub bucket_timestamp: Timestamp,
    pub min_pos: Cursor,
    pub max_pos: Cursor,
    pub row_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct PrunedBatches {
    pub stored: Vec<BatchId>,
    pub inmem: Vec<InMemBatchId>,
}

/// A bucket containing batches with the same bucket timestamp.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bucket {
    pub timestamp: Timestamp,
    /// Hot, in-memory base segments for fast reads.
    ///
    /// Invariant: these runs are `BatchRef::InMem` only.
    #[serde(skip)]
    pub hot_base_segments: Vec<BatchMeta>,
    /// Hot, in-memory delta batches appended on ingest.
    ///
    /// Invariant: these runs are `BatchRef::InMem` only.
    #[serde(skip)]
    pub hot_deltas: Vec<BatchMeta>,
    /// Persisted, store-backed layout for this bucket.
    ///
    /// This is updated by the dumper/checkpoint path.
    #[serde(default)]
    pub persisted_segments: Vec<BatchMeta>,
    /// Stored runs that became unreachable due to publishing an in-mem layout.
    ///
    /// These must be deleted by the dumper/pruner path (read-compaction intentionally doesn't touch the store).
    #[serde(default)]
    pub pending_delete_stored: Vec<BatchId>,
    pub row_count: usize,
    /// Version that bumps when new data arrives or old data is pruned.
    ///
    /// Kept as `version` for backward compatibility with existing cache keys/tests.
    pub version: u64,
}

impl Bucket {
    pub fn new(timestamp: Timestamp) -> Self {
        Self {
            timestamp,
            hot_base_segments: Vec::new(),
            hot_deltas: Vec::new(),
            persisted_segments: Vec::new(),
            pending_delete_stored: Vec::new(),
            row_count: 0,
            version: 0,
        }
    }

    pub fn push_hot_delta(&mut self, metadata: BatchMeta) {
        debug_assert!(metadata.run.inmem_id().is_some(), "hot runs must be in-mem");
        self.row_count += metadata.row_count;
        self.hot_deltas.push(metadata);
        self.version = self.version.wrapping_add(1);
    }

    /// Predicate used by the read path to decide whether it should compact this bucket
    /// before building a `SortedRangeView`.
    pub fn should_compact(&self) -> bool {
        const MAX_DELTA_RUNS_BEFORE_COMPACT: usize = 8;

        if self.hot_deltas.is_empty() {
            return false;
        }
        if self.hot_deltas.len() > MAX_DELTA_RUNS_BEFORE_COMPACT {
            return true;
        }

        let mut metas: Vec<&BatchMeta> = self
            .hot_base_segments
            .iter()
            .chain(self.hot_deltas.iter())
            .collect();
        if metas.len() <= 1 {
            return false;
        }
        metas.sort_by(|a, b| a.min_pos.cmp(&b.min_pos));
        for w in metas.windows(2) {
            if w[1].min_pos <= w[0].max_pos {
                return true;
            }
        }
        false
    }
}


// TODO this should not be clonable?
/// Bucket-level index using bucket timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

        self.insert_batch_ref(bucket_ts, BatchRef::Stored(batch_id), min_pos, max_pos, row_count);
    }

    pub fn insert_batch_ref(
        &mut self,
        bucket_ts: Timestamp,
        run: BatchRef,
        min_pos: Cursor,
        max_pos: Cursor,
        row_count: usize,
    ) {
        let metadata = BatchMeta {
            run,
            bucket_timestamp: bucket_ts,
            min_pos,
            max_pos,
            row_count,
        };

        let b = self
            .buckets
            .entry(bucket_ts)
            .or_insert_with(|| Bucket::new(bucket_ts));

        match metadata.run {
            BatchRef::InMem(_) => {
                b.push_hot_delta(metadata);
            }
            BatchRef::Stored(_) => {
                // Treat as canonical persisted data insertion (mostly used by tests).
                b.row_count += metadata.row_count;
                b.persisted_segments.push(metadata);
                b.version = b.version.wrapping_add(1);
            }
        }

        self.total_rows += row_count;
        self.max_pos_seen = self.max_pos_seen.max(max_pos);
    }

    /// Query buckets with bucket_timestamp in range [start, end]
    pub fn query_buckets_in_range(&self, start: Timestamp, end: Timestamp) -> Vec<&Bucket> {
        self.buckets.range(start..=end).map(|(_, b)| b).collect()
    }

    pub fn bucket(&self, bucket_ts: Timestamp) -> Option<&Bucket> {
        self.buckets.get(&bucket_ts)
    }

    pub fn bucket_mut(&mut self, bucket_ts: Timestamp) -> Option<&mut Bucket> {
        self.buckets.get_mut(&bucket_ts)
    }

    /// Bucket-level span that may contain rows in (prev, new] (by cursor order).
    ///
    /// This is used to plan "entry buckets" for plain and retractable streaming aggregates.
    pub fn delta_span(&self, prev: Option<Cursor>, new: Cursor) -> Option<BucketRange> {
        if self.buckets.is_empty() {
            return None;
        }
        let prev = prev.unwrap_or(Cursor::new(i64::MIN, 0));
        if new <= prev {
            return None;
        }

        let end_bucket_ts = self.bucket_granularity.start(new.ts);
        let start_bucket_ts = if prev.ts == i64::MIN {
            self.buckets.keys().next().copied().unwrap_or(end_bucket_ts)
        } else {
            self.bucket_granularity.start(prev.ts)
        };

        let mut iter = self.buckets.range(start_bucket_ts..=end_bucket_ts);
        let first = iter.next().map(|(k, _)| *k)?;
        let last = self
            .buckets
            .range(start_bucket_ts..=end_bucket_ts)
            .next_back()
            .map(|(k, _)| *k)?;
        Some(BucketRange::new(first, last))
    }

    /// Bucket range that covers the full time window context needed for RANGE windows.
    pub fn bucket_span_for_range_window(&self, end_span: BucketRange, window_length_ms: i64) -> BucketRange {
        if self.buckets.is_empty() {
            return end_span;
        }
        let start_unaligned = end_span.start.saturating_sub(window_length_ms);
        let start = self.bucket_granularity.start(start_unaligned);
        let end = end_span.end;

        let mut iter = self.buckets.range(start..=end);
        let first = iter.next().map(|(k, _)| *k);
        let last = self.buckets.range(start..=end).next_back().map(|(k, _)| *k);
        match (first, last) {
            (Some(s), Some(e)) => BucketRange::new(s, e),
            _ => end_span,
        }
    }

    /// Bucket range that covers the full context needed for ROWS windows (bucket-count approximation).
    pub fn bucket_span_for_rows_window(&self, end_span: BucketRange, window_size: usize) -> BucketRange {
        if self.buckets.is_empty() {
            return end_span;
        }

        // Include all buckets in [start,end]
        let mut rows_in_range: usize = 0;
        for (_, bucket) in self.buckets.range(end_span.start..=end_span.end) {
            rows_in_range += bucket.row_count;
        }

        let mut start = end_span.start;
        if rows_in_range < window_size {
            let mut rows_needed = window_size - rows_in_range;
            for (&bucket_ts, bucket) in self.buckets.range(..end_span.start).rev() {
                start = bucket_ts;
                if rows_needed <= bucket.row_count {
                    break;
                }
                rows_needed -= bucket.row_count;
            }
        }

        // Clamp start/end to existing buckets (defensive).
        let end = end_span.end;
        let first = self.buckets.range(start..=end).next().map(|(k, _)| *k);
        let last = self.buckets.range(start..=end).next_back().map(|(k, _)| *k);
        match (first, last) {
            (Some(s), Some(e)) => BucketRange::new(s, e),
            _ => end_span,
        }
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

    pub fn prune(&mut self, cutoff_timestamp: Timestamp) -> PrunedBatches {
        let mut pruned = PrunedBatches::default();
        let mut empty_buckets = Vec::new();

        for (&bucket_ts, bucket) in self.buckets.iter_mut() {
            // Prune hot in-mem base (counts towards logical rows when it exists).
            let before_base_len = bucket.hot_base_segments.len();
            bucket.hot_base_segments.retain(|b| {
                if b.max_pos.ts < cutoff_timestamp {
                    if let Some(id) = b.run.inmem_id() {
                        pruned.inmem.push(id);
                    }
                    self.total_rows -= b.row_count;
                    bucket.row_count -= b.row_count;
                    false
                } else {
                    true
                }
            });
            let hot_base_pruned = bucket.hot_base_segments.len() != before_base_len;

            // Prune hot deltas (always counted).
            let before_len = bucket.hot_deltas.len();
            bucket.hot_deltas.retain(|b| {
                if b.max_pos.ts < cutoff_timestamp {
                    if let Some(id) = b.run.inmem_id() {
                        pruned.inmem.push(id);
                    }
                    self.total_rows -= b.row_count;
                    bucket.row_count -= b.row_count;
                    false
                } else {
                    true
                }
            });
            if bucket.hot_deltas.len() != before_len || hot_base_pruned {
                bucket.version = bucket.version.wrapping_add(1);
            }

            // Always prune persisted store-backed segments (do NOT double-decrement row_count if hot base exists).
            let before_persisted = bucket.persisted_segments.len();
            bucket.persisted_segments.retain(|m| {
                if m.max_pos.ts < cutoff_timestamp {
                    if let Some(id) = m.run.stored_batch_id() {
                        pruned.stored.push(id);
                    }
                    if !hot_base_pruned && bucket.hot_base_segments.is_empty() {
                        // Only adjust counts if persisted is the canonical representation.
                        self.total_rows -= m.row_count;
                        bucket.row_count -= m.row_count;
                    }
                    false
                } else {
                    true
                }
            });
            if bucket.persisted_segments.len() != before_persisted && bucket.hot_base_segments.is_empty() {
                bucket.version = bucket.version.wrapping_add(1);
            }

            if bucket.hot_deltas.is_empty() && bucket.hot_base_segments.is_empty() && bucket.persisted_segments.is_empty() {
                // If the bucket is becoming empty, also retire any pending stored ids.
                if !bucket.pending_delete_stored.is_empty() {
                    pruned.stored.extend(bucket.pending_delete_stored.drain(..));
                }
                empty_buckets.push(bucket_ts);
            }
        }

        for bucket_ts in empty_buckets {
            self.buckets.remove(&bucket_ts);
        }

        pruned
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
        // Stored inserts are not hot; they land in persisted layout.
        assert_eq!(bucket.persisted_segments.len(), 2);
    }

    #[test]
    fn test_prune() {
        let mut index = BucketIndex::new(TimeGranularity::Seconds(1));

        index.insert_batch(make_batch_id(1000, 1), make_pos(1000, 1), make_pos(1500, 100), 100);
        index.insert_batch(make_batch_id(2000, 2), make_pos(2000, 101), make_pos(2500, 150), 50);
        index.insert_batch(make_batch_id(3000, 3), make_pos(3000, 151), make_pos(3500, 225), 75);

        let pruned = index.prune(2000);
        assert_eq!(pruned.stored.len(), 1);
        assert!(pruned.inmem.is_empty());
        assert_eq!(index.total_rows(), 125);
        assert_eq!(index.bucket_timestamps(), vec![2000, 3000]);
    }
}


