use std::cmp::Ordering;
use std::collections::BinaryHeap;

use arrow::array::{ArrayRef, TimestampMillisecondArray, UInt32Array, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::storage::index::Cursor;

/// Compact a set of batches into sorted, disjoint segments.
///
/// Implementation:
/// - If inputs are already disjoint by cursor range, returns them in order (no copy).
/// - Otherwise performs a k-way merge of already-sorted runs (O(n log k)) to build a merged batch.
/// - Splits output into `max_rows_per_segment` slices (slices are zero-copy views)
pub fn compact_to_disjoint_segments(
    batches: &[RecordBatch],
    ts_column_index: usize,
    seq_column_index: usize,
    max_rows_per_segment: usize,
) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }
    if batches.len() == 1 {
        return split_into_segments(batches[0].clone(), max_rows_per_segment);
    }

    let schema: SchemaRef = batches[0].schema();
    for b in batches.iter().skip(1) {
        if b.schema() != schema {
            panic!("compact_to_disjoint_segments requires identical schemas");
        }
    }

    // Fast path: disjoint, ordered inputs => return in order (no copy).
    let mut ranges: Vec<(Cursor, Cursor, usize)> = Vec::with_capacity(batches.len());
    for (i, b) in batches.iter().enumerate() {
        if b.num_rows() == 0 {
            continue;
        }
        let (min, max) = first_last_cursor(b, ts_column_index, seq_column_index);
        ranges.push((min, max, i));
    }
    if ranges.is_empty() {
        return Vec::new();
    }
    ranges.sort_by(|a, b| a.0.cmp(&b.0));
    for w in ranges.windows(2) {
        if w[1].0 <= w[0].1 {
            return split_into_segments(
                k_way_merge_to_batch(batches, ts_column_index, seq_column_index, schema),
                max_rows_per_segment,
            );
        }
    }
    let mut out: Vec<RecordBatch> = Vec::new();
    for (_, _, idx) in ranges {
        out.extend(split_into_segments(batches[idx].clone(), max_rows_per_segment));
    }
    out
}

fn split_into_segments(batch: RecordBatch, max_rows_per_segment: usize) -> Vec<RecordBatch> {
    let max_rows_per_segment = max_rows_per_segment.max(1);
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(max_rows_per_segment);
        out.push(batch.slice(offset, len));
        offset += len;
    }
    out
}

fn first_last_cursor(batch: &RecordBatch, ts_idx: usize, seq_idx: usize) -> (Cursor, Cursor) {
    let ts = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("timestamp column must be TimestampMillisecondArray");
    let seq = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("__seq_no column must be UInt64Array");
    let n = batch.num_rows();
    (
        Cursor::new(ts.value(0), seq.value(0)),
        Cursor::new(ts.value(n - 1), seq.value(n - 1)),
    )
}

#[derive(Clone, Debug)]
struct HeapItem {
    cursor: Cursor,
    batch_idx: usize,
    row_idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cursor
            .cmp(&self.cursor)
            .then_with(|| other.batch_idx.cmp(&self.batch_idx))
            .then_with(|| other.row_idx.cmp(&self.row_idx))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cursor == other.cursor && self.batch_idx == other.batch_idx && self.row_idx == other.row_idx
    }
}

impl Eq for HeapItem {}

fn k_way_merge_to_batch(
    batches: &[RecordBatch],
    ts_idx: usize,
    seq_idx: usize,
    schema: SchemaRef,
) -> RecordBatch {
    let mut ts_cols: Vec<&TimestampMillisecondArray> = Vec::with_capacity(batches.len());
    let mut seq_cols: Vec<&UInt64Array> = Vec::with_capacity(batches.len());
    let mut offsets: Vec<u32> = Vec::with_capacity(batches.len());
    let mut total_rows: usize = 0;
    for b in batches {
        offsets.push(total_rows as u32);
        total_rows += b.num_rows();
        ts_cols.push(
            b.column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp column must be TimestampMillisecondArray"),
        );
        seq_cols.push(
            b.column(seq_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("__seq_no column must be UInt64Array"),
        );
    }

    let concatenated = arrow::compute::concat_batches(&schema, batches).expect("concat batches");

    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (i, b) in batches.iter().enumerate() {
        if b.num_rows() == 0 {
            continue;
        }
        heap.push(HeapItem {
            cursor: Cursor::new(ts_cols[i].value(0), seq_cols[i].value(0)),
            batch_idx: i,
            row_idx: 0,
        });
    }

    let mut gather: Vec<u32> = Vec::with_capacity(total_rows);
    while let Some(item) = heap.pop() {
        gather.push(offsets[item.batch_idx] + item.row_idx as u32);
        let next_row = item.row_idx + 1;
        if next_row < batches[item.batch_idx].num_rows() {
            heap.push(HeapItem {
                cursor: Cursor::new(ts_cols[item.batch_idx].value(next_row), seq_cols[item.batch_idx].value(next_row)),
                batch_idx: item.batch_idx,
                row_idx: next_row,
            });
        }
    }

    let indices = UInt32Array::from(gather);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(concatenated.num_columns());
    for col in concatenated.columns() {
        cols.push(take(col.as_ref(), &indices, None).expect("take merged indices"));
    }
    RecordBatch::try_new(schema, cols).expect("merged record batch")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn make_batch(ts: &[i64], seq: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("__seq_no", DataType::UInt64, false),
        ]));
        let ts_arr = TimestampMillisecondArray::from(ts.to_vec());
        let seq_arr = UInt64Array::from(seq.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(seq_arr)]).unwrap()
    }

    #[test]
    fn disjoint_fast_path_preserves_inputs() {
        let b1 = make_batch(&[0, 1, 2], &[0, 1, 2]);
        let b2 = make_batch(&[10, 11], &[3, 4]);
        let out = compact_to_disjoint_segments(&[b2.clone(), b1.clone()], 0, 1, 10);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows(), b1.num_rows());
        assert_eq!(out[1].num_rows(), b2.num_rows());
    }

    #[test]
    fn overlap_merges_and_sorts() {
        let b1 = make_batch(&[0, 2, 4], &[0, 2, 4]);
        let b2 = make_batch(&[1, 3, 5], &[1, 3, 5]);
        let out = compact_to_disjoint_segments(&[b1, b2], 0, 1, 10);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 6);
        let ts = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        for i in 1..ts.len() {
            assert!(ts.value(i - 1) <= ts.value(i));
        }
    }
}

