//! WO emit helpers: which ends fire and their input-row scalars.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::row_nav::flatten_ordered;

/// Emit ends in `(prev, advance_to]` from loaded batches.
pub(super) fn emit_ends(
    batches: &[RecordBatch],
    ts_column_index: usize,
    prev: Option<Cursor>,
    advance_to: Cursor,
) -> Vec<Cursor> {
    let prev = prev.unwrap_or(Cursor::new(i64::MIN, 0));
    flatten_ordered(batches, ts_column_index)
        .into_iter()
        .map(|(c, _, _)| c)
        .filter(|c| *c > prev && *c <= advance_to)
        .collect()
}

/// Map emit ends → input row scalars. Missing cursor → nulls.
pub(super) fn emit_input_rows(
    batches: &[RecordBatch],
    ts_column_index: usize,
    emit_cursors: &[Cursor],
    input_schema: &SchemaRef,
) -> Result<Vec<Vec<ScalarValue>>> {
    let entries = flatten_ordered(batches, ts_column_index);
    let mut out = Vec::with_capacity(emit_cursors.len());
    let mut i = 0usize;
    for c in emit_cursors {
        while i < entries.len() && entries[i].0 < *c {
            i += 1;
        }
        let Some(&(_, ci, ri)) = entries.get(i).filter(|(ec, _, _)| *ec == *c) else {
            out.push(vec![ScalarValue::Null; input_schema.fields().len()]);
            continue;
        };
        let mut vals = Vec::new();
        for col_idx in 0..input_schema.fields().len() {
            vals.push(ScalarValue::try_from_array(
                batches[ci as usize].column(col_idx),
                ri as usize,
            )?);
        }
        out.push(vals);
        i += 1;
    }
    Ok(out)
}
