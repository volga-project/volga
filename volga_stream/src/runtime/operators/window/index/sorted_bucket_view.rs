use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, TimestampMillisecondArray, UInt64Array};

use crate::runtime::operators::window::Cursor;
use crate::storage::batch_store::{BatchId, Timestamp};

use super::Bucket;
use crate::runtime::operators::window::aggregates::arrow_utils::sort_batch_by_timestamp;

#[derive(Debug, Clone)]
pub struct SortedBucketView {
    bucket_ts: Timestamp,
    batch: RecordBatch,
    ts_column_index: usize,
    seq_column_index: usize,
    /// Pre-evaluated window args keyed by window_id.
    ///
    /// TODO: This can be wasteful if multiple windows have identical argument sets
    /// (same expr or same input columns). We can dedupe by keying on a stable "args signature"
    /// instead of window_id (e.g. expr digest + input column indices).
    bucket_args: HashMap<usize, Arc<Vec<ArrayRef>>>,
}

impl SortedBucketView {
    pub fn new(
        bucket_ts: Timestamp,
        batch: RecordBatch,
        ts_column_index: usize,
        seq_column_index: usize,
        bucket_args: HashMap<usize, Arc<Vec<ArrayRef>>>,
    ) -> Self {
        Self {
            bucket_ts,
            batch,
            ts_column_index,
            seq_column_index,
            bucket_args,
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

    pub fn args(&self, window_id: usize) -> &Arc<Vec<ArrayRef>> {
        self.bucket_args
            .get(&window_id)
            .expect("Bucket args should be initialized for this window_id")
    }
}

/// Sort batches within each bucket and return vec of sorted batches
/// (one per bucket, ordered by bucket timestamp).
pub fn sort_buckets(
    buckets: &[Bucket],
    batches: &HashMap<BatchId, RecordBatch>,
    ts_column_index: usize,
) -> Vec<RecordBatch> {
    let mut result = Vec::new();

    for bucket in buckets {
        let bucket_batches: Vec<&RecordBatch> = bucket
            .batches
            .iter()
            .filter_map(|m| batches.get(&m.batch_id))
            .collect();

        if bucket_batches.is_empty() {
            continue;
        }

        if bucket_batches.len() == 1 {
            result.push(sort_batch_by_timestamp(bucket_batches[0], ts_column_index));
        } else {
            // TODO: if we pre-sort batches during insertion, we can just merge here instead of sorting again
            let schema = bucket_batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema, bucket_batches)
                .expect("Should be able to concatenate batches within bucket");
            result.push(sort_batch_by_timestamp(&concatenated, ts_column_index));
        }
    }

    result
}

