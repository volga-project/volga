use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

// copied from private DataFusion function
pub fn create_output_schema(input_schema: &Schema, window_exprs: &[Arc<dyn WindowExpr>]) -> Arc<Schema> {
    let capacity = input_schema.fields().len() + window_exprs.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    for expr in window_exprs {
        builder.push(expr.field().expect("Should be able to get field"));
    }
    Arc::new(builder.finish().with_metadata(input_schema.metadata().clone()))
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

// creates a record batch by vertically stacking input_values and aggregated_values
pub fn stack_concat_results(
    input_values: Vec<Vec<ScalarValue>>,      // input columns (per row)
    aggregated_values: Vec<Vec<ScalarValue>>, // window result columns (per row, per window expr)
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> RecordBatch {
    let num_rows = input_values.len();

    // Filter out rows where any window output is Null (too-late entries).
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

