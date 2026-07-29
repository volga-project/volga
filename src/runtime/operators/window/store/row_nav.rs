//! In-memory cursor-ordered navigation over raw Arrow batches.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, RecordBatch, TimestampMillisecondArray, UInt64Array};
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdx(pub usize);

#[derive(Debug)]
pub struct RowNav {
    /// Flat index: global row → (batch, row-in-batch).
    locs: Vec<(u32, u32)>,
    cursors: Vec<Cursor>,
    /// `evaluate_args` once per batch.
    batch_args: Vec<Vec<ArrayRef>>,
}

impl RowNav {
    pub fn from_batches(
        batches: &[RecordBatch],
        ts_column_index: usize,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> Self {
        let entries = flatten_ordered(batches, ts_column_index);
        let mut locs = Vec::with_capacity(entries.len());
        let mut cursors = Vec::with_capacity(entries.len());
        for (c, ci, ri) in entries {
            locs.push((ci, ri));
            cursors.push(c);
        }

        let batch_args: Vec<_> = batches
            .iter()
            .map(|batch| {
                if batch.num_rows() == 0 {
                    Vec::new()
                } else {
                    window_expr.evaluate_args(batch).expect("evaluate_args")
                }
            })
            .collect();

        Self {
            locs,
            cursors,
            batch_args,
        }
    }

    pub fn cursor(&self, idx: RowIdx) -> Cursor {
        self.cursors[idx.0]
    }

    /// Per-row args sliced from the batch-level `evaluate_args`.
    pub fn args(&self, idx: RowIdx) -> Vec<ArrayRef> {
        let (ci, ri) = self.locs[idx.0];
        self.batch_args[ci as usize]
            .iter()
            .map(|array| array.slice(ri as usize, 1))
            .collect()
    }

    pub fn next(&self, idx: RowIdx) -> Option<RowIdx> {
        let n = idx.0 + 1;
        if n < self.locs.len() {
            Some(RowIdx(n))
        } else {
            None
        }
    }

    /// Last row with cursor <= target.
    pub fn seek_le(&self, target: Cursor) -> Option<RowIdx> {
        match self.cursors.iter().rposition(|c| *c <= target) {
            Some(i) => Some(RowIdx(i)),
            None => None,
        }
    }

    /// First index with cursor >= target.
    pub fn seek_ge(&self, target: Cursor) -> Option<RowIdx> {
        self.cursors.iter().position(|c| *c >= target).map(RowIdx)
    }

    /// First index with ts >= start_ts (for RANGE window start).
    pub fn seek_ts_ge(&self, start_ts: i64) -> Option<RowIdx> {
        self.cursors
            .iter()
            .position(|c| c.ts >= start_ts)
            .map(RowIdx)
    }
}

/// Flatten batches into cursor-ordered `(cursor, batch_idx, row_idx)` entries.
pub(crate) fn flatten_ordered(
    batches: &[RecordBatch],
    ts_column_index: usize,
) -> Vec<(Cursor, u32, u32)> {
    let mut entries = Vec::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let cursors =
            cursors_from_batch(batch, ts_column_index).expect("stored batch cursor columns");
        entries.extend(
            cursors
                .into_iter()
                .enumerate()
                .map(|(row_idx, cursor)| (cursor, batch_idx as u32, row_idx as u32)),
        );
    }
    entries.sort_by_key(|(cursor, _, _)| *cursor);
    entries.dedup_by_key(|(cursor, _, _)| *cursor);
    entries
}

pub(crate) fn cursors_from_batch(
    batch: &RecordBatch,
    ts_column_index: usize,
) -> Result<Vec<Cursor>> {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow!("event-time column must use millisecond precision"))?;
    let seq_idx = batch.schema().index_of(SEQ_NO_COLUMN_NAME)?;
    let seq = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("invalid {SEQ_NO_COLUMN_NAME} column"))?;
    Ok((0..batch.num_rows())
        .map(|row| Cursor::new(ts.value(row), seq.value(row)))
        .collect())
}
