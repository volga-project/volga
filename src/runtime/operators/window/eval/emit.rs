//! WO emit helpers: which ends fire and their input-row scalars.

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::event_chunk::{flatten_ordered, EventChunk};

/// Emit ends in `(prev, advance_to]` from loaded chunks (must be Cursor-ordered).
pub(super) fn emit_ends(
    chunks: &[EventChunk],
    prev: Option<Cursor>,
    advance_to: Cursor,
) -> Vec<Cursor> {
    let prev = prev.unwrap_or(Cursor::new(i64::MIN, 0));
    flatten_ordered(chunks)
        .into_iter()
        .map(|(c, _, _)| c)
        .filter(|c| *c > prev && *c <= advance_to)
        .collect()
}

/// Map emit ends → input row scalars. Missing cursor → nulls.
pub(super) fn emit_input_rows(
    chunks: &[EventChunk],
    emit_cursors: &[Cursor],
    input_schema: &SchemaRef,
) -> Result<Vec<Vec<ScalarValue>>> {
    let entries = flatten_ordered(chunks);
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
                chunks[ci as usize].batch.column(col_idx),
                ri as usize,
            )?);
        }
        out.push(vals);
        i += 1;
    }
    Ok(out)
}
