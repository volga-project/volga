//! Assemble WO/WRO output batches from input scalars + per-window values.

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

/// Build an output batch from per-row input scalars and per-window result columns.
///
/// `aggregated_values` is `[window_id][row]` (not row-major).
/// Rows where any window value is Null are dropped (all windows must agree).
pub fn assemble_window_batch(
    input_values: Vec<Vec<ScalarValue>>,
    aggregated_values: Vec<Vec<ScalarValue>>,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> RecordBatch {
    let num_rows = input_values.len();

    let keep_indices = select_keep_indices_for_non_null_rows(&aggregated_values, num_rows);
    if keep_indices.is_empty() {
        return RecordBatch::new_empty(output_schema.clone());
    }

    let filtered_input_values = filter_rows(input_values, &keep_indices);
    let filtered_aggregated_values = filter_rows_2d(aggregated_values, &keep_indices);

    build_record_batch_from_scalar_columns(
        filtered_input_values,
        filtered_aggregated_values,
        output_schema,
        input_schema,
    )
}

fn select_keep_indices_for_non_null_rows(
    aggregated_values: &[Vec<ScalarValue>],
    num_rows: usize,
) -> Vec<usize> {
    (0..num_rows)
        .filter(|&row_idx| {
            let has_null = aggregated_values.iter().any(|window_results| {
                matches!(window_results.get(row_idx), Some(ScalarValue::Null))
            });

            if has_null {
                for window_results in aggregated_values {
                    assert!(
                        matches!(window_results.get(row_idx), Some(ScalarValue::Null)),
                        "If any window has Null for a row, all windows must have Null (row_idx: {})",
                        row_idx
                    );
                }
                false
            } else {
                true
            }
        })
        .collect()
}

fn filter_rows<T: Clone>(values: Vec<T>, keep_indices: &[usize]) -> Vec<T> {
    keep_indices.iter().map(|&idx| values[idx].clone()).collect()
}

fn filter_rows_2d<T: Clone>(values: Vec<Vec<T>>, keep_indices: &[usize]) -> Vec<Vec<T>> {
    values
        .into_iter()
        .map(|col| keep_indices.iter().map(|&idx| col[idx].clone()).collect())
        .collect()
}

fn build_record_batch_from_scalar_columns(
    input_values: Vec<Vec<ScalarValue>>,
    aggregated_values: Vec<Vec<ScalarValue>>,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for col_idx in 0..input_schema.fields().len() {
        let column_values: Vec<ScalarValue> =
            input_values.iter().map(|row| row[col_idx].clone()).collect();
        let array = ScalarValue::iter_to_array(column_values.into_iter())
            .expect("Should be able to convert input values to array");
        columns.push(array);
    }

    for window_results in aggregated_values.iter() {
        let array = ScalarValue::iter_to_array(window_results.iter().cloned())
            .expect("Should be able to convert scalar values to array");
        columns.push(array);
    }

    if columns.len() != output_schema.fields().len() {
        panic!(
            "Mismatch between number of result columns ({}) and schema fields ({})",
            columns.len(),
            output_schema.fields().len()
        );
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .expect("Should be able to create RecordBatch from window results")
}
