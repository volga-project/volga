use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use crate::storage::batch_store::BatchId;

/// Sort a RecordBatch by timestamp column using Arrow kernels
pub fn sort_batch_by_timestamp(batch: &RecordBatch, ts_column_index: usize) -> RecordBatch {
    use arrow::compute::kernels::sort::{sort_to_indices, SortOptions};
    use arrow::compute::take_record_batch;
    
    if batch.num_rows() <= 1 {
        return batch.clone();
    }
    
    let ts_column = batch.column(ts_column_index);
    
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    
    let indices = sort_to_indices(ts_column, Some(sort_options), None)
        .expect("Should be able to sort by timestamp");
    
    take_record_batch(batch, &indices)
        .expect("Should be able to reorder batch by sort indices")
}

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

// Core batch evaluation logic - take batch rows and evaluates args based on window expr
pub fn evalute_batch(
    batch: &RecordBatch,
    positions_and_rows: &[(usize, usize)], // (original_position, row_idx in this batch)
    window_expr: &Arc<dyn WindowExpr>
) -> Result<Vec<arrow::array::ArrayRef>, Box<dyn std::error::Error>> {
    use arrow::array::UInt32Array;
    use arrow::compute::take_record_batch;

    // Extract row indices for this batch
    let row_indices: Vec<u32> = positions_and_rows
        .iter()
        .map(|&(_, row_idx)| row_idx as u32)
        .collect();
    let indices_array = UInt32Array::from(row_indices);
    
    // Take rows from the batch
    let taken_batch = take_record_batch(batch, &indices_array)?;
    
    // Evaluate args on taken batch
    let batch_args = window_expr.evaluate_args(&taken_batch)?;
    
    Ok(batch_args)
}

// Core batch evaluation logic - evaluates batches and maps results back to positions
pub fn evaluate_batches_and_map_results<F>(
    indices_per_batch: HashMap<BatchId, Vec<(usize, usize)>>, // (position, row_idx)
    batches: &HashMap<BatchId, RecordBatch>,
    window_expr: &Arc<dyn WindowExpr>,
    mut result_handler: F
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut(usize, Vec<arrow::array::ArrayRef>) // (position, single_row_args)
{
    // Process each batch
    for (batch_id, positions_and_rows) in indices_per_batch {
        let batch = batches.get(&batch_id).expect("Batch should exist");
        let batch_args = evalute_batch(batch, &positions_and_rows, window_expr)?;
        
        // Map results back to original positions
        for (filtered_row_idx, &(original_pos, _row_idx)) in positions_and_rows.iter().enumerate() {
            if filtered_row_idx < batch_args[0].len() {
                // Extract single row args for this position
                let single_row_args: Vec<arrow::array::ArrayRef> = batch_args
                    .iter()
                    .map(|array| array.slice(filtered_row_idx, 1))
                    .collect();
                
                result_handler(original_pos, single_row_args);
            }
        }
    }
    
    Ok(())
}