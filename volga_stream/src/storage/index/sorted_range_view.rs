use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, TimestampMillisecondArray, UInt64Array};

use crate::runtime::operators::window::aggregates::BucketRange;
use crate::runtime::operators::window::state::tiles::TimeGranularity;
use crate::runtime::operators::window::Cursor;
use crate::storage::batch_store::Timestamp;
use crate::storage::WorkLease;

#[derive(Debug, Clone, Copy)]
pub enum DataBounds {
    /// Load all runs in the requested bucket range.
    All,
    /// Time-bounded request (RANGE windows); loader may prune runs by overlap.
    Time { start_ts: Timestamp, end_ts: Timestamp },
    /// ROWS windows request: load enough data to cover the last `rows` ending at `end_ts`.
    ///
    /// Loader computes bucket coverage by bucket counts, then may prune within the partial bucket
    /// using run-interval overlap components.
    RowsTail { end_ts: Timestamp, rows: usize },
}

#[derive(Debug, Clone, Copy)]
pub struct DataRequest {
    pub bucket_range: BucketRange,
    pub bounds: DataBounds,
}

/// A sorted run/segment + pre-evaluated args for range-view execution.
///
/// Range views standardize args under window_id=0, so segments store a single args vector.
#[derive(Debug, Clone)]
pub struct SortedSegment {
    bucket_ts: Timestamp,
    batch: RecordBatch,
    ts_column_index: usize,
    seq_column_index: usize,
    args: Arc<Vec<ArrayRef>>,
}

impl SortedSegment {
    pub fn new(
        bucket_ts: Timestamp,
        batch: RecordBatch,
        ts_column_index: usize,
        seq_column_index: usize,
        args: Arc<Vec<ArrayRef>>,
    ) -> Self {
        Self {
            bucket_ts,
            batch,
            ts_column_index,
            seq_column_index,
            args,
        }
    }

    pub fn bucket_ts(&self) -> Timestamp {
        self.bucket_ts
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn size(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn timestamps(&self) -> &TimestampMillisecondArray {
        self.batch
            .column(self.ts_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Timestamp column should be TimestampMillisecondArray")
    }

    pub fn timestamp(&self, row: usize) -> Timestamp {
        self.timestamps().value(row)
    }

    pub fn seq_nos(&self) -> &UInt64Array {
        self.batch
            .column(self.seq_column_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("__seq_no column should be UInt64Array")
    }

    pub fn row_pos(&self, row: usize) -> Cursor {
        Cursor::new(self.timestamp(row), self.seq_nos().value(row))
    }

    pub fn args(&self) -> &Arc<Vec<ArrayRef>> {
        &self.args
    }
}

/// A raw-data view for a single contiguous bucket range, with row-level bounds.
///
/// First implementation is backed by full-bucket sorted batches (no physical IO pruning yet).
#[derive(Debug, Clone)]
pub struct SortedRangeView {
    request: DataRequest,
    bucket_granularity: TimeGranularity,
    start: Cursor,
    end: Cursor,
    segments: Vec<SortedSegment>,
    // Keeps work-memory permit alive for the lifetime of this view.
    #[allow(dead_code)]
    work_lease: Option<WorkLease>,
}

impl SortedRangeView {
    pub(crate) fn new(
        request: DataRequest,
        bucket_granularity: TimeGranularity,
        start: Cursor,
        end: Cursor,
        segments: Vec<SortedSegment>,
        work_lease: Option<WorkLease>,
    ) -> Self {
        Self {
            request,
            bucket_granularity,
            start,
            end,
            segments,
            work_lease,
        }
    }

    pub fn bucket_range(&self) -> BucketRange {
        self.request.bucket_range
    }

    pub fn start(&self) -> Cursor {
        self.start
    }

    pub fn end(&self) -> Cursor {
        self.end
    }

    pub fn bounds(&self) -> DataBounds {
        self.request.bounds
    }

    pub fn bucket_granularity(&self) -> TimeGranularity {
        self.bucket_granularity
    }

    pub(crate) fn segments(&self) -> &[SortedSegment] {
        &self.segments
    }
}

