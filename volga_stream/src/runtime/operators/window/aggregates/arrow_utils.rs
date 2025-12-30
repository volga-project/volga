
use arrow::array::RecordBatch;

/// Sort a RecordBatch by (timestamp, seq_no) using Arrow kernels.
pub fn sort_batch_by_rowpos(
    batch: &RecordBatch,
    ts_column_index: usize,
    seq_column_index: usize,
) -> RecordBatch {
    use arrow::compute::kernels::sort::{lexsort_to_indices, SortColumn, SortOptions};
    use arrow::compute::take_record_batch;

    if batch.num_rows() <= 1 {
        return batch.clone();
    }

    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let ts_col = SortColumn {
        values: batch.column(ts_column_index).clone(),
        options: Some(sort_options),
    };
    let seq_col = SortColumn {
        values: batch.column(seq_column_index).clone(),
        options: Some(sort_options),
    };

    let indices = lexsort_to_indices(&[ts_col, seq_col], None)
        .expect("Should be able to lexsort by (ts, seq_no)");

    take_record_batch(batch, &indices).expect("Should be able to reorder batch by sort indices")
}
