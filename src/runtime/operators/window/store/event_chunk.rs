//! Event chunk (Arrow batch + per-row cursors).
//!
//! Writes are one row per KV entry today; multi-row packing is reserved
//! ([#155](https://github.com/volga-project/volga/issues/155)). Slicing still
//! needed for bucket overscan.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{RecordBatch, TimestampMillisecondArray, UInt64Array};

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;

/// One contiguous Arrow batch of events, cursor-ordered.
#[derive(Debug, Clone)]
pub struct EventChunk {
    pub batch: Arc<RecordBatch>,
    /// Parallel to rows; same order as `batch`.
    pub cursors: Arc<[Cursor]>,
}

impl EventChunk {
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Build from a batch that already has ts + `__seq_no`.
    pub fn from_batch(batch: RecordBatch, ts_column_index: usize) -> Result<Self> {
        let cursors = cursors_from_batch(&batch, ts_column_index)?;
        Ok(Self {
            batch: Arc::new(batch),
            cursors: cursors.into(),
        })
    }

    /// Zero-copy row slice `[s, e]` inclusive.
    pub fn slice_rows(&self, s: usize, e: usize) -> Self {
        debug_assert!(s <= e && e < self.num_rows());
        if s == 0 && e + 1 == self.num_rows() {
            return self.clone();
        }
        Self {
            batch: Arc::new(self.batch.slice(s, e - s + 1)),
            cursors: self.cursors[s..=e].to_vec().into(),
        }
    }

    /// Rows with `from <= cursor <= to`.
    pub fn slice_inclusive(&self, from: Cursor, to: Cursor) -> Option<Self> {
        let mut start = None;
        let mut end = None;
        for (i, c) in self.cursors.iter().enumerate() {
            if *c >= from && *c <= to {
                if start.is_none() {
                    start = Some(i);
                }
                end = Some(i);
            }
        }
        let (s, e) = match (start, end) {
            (Some(s), Some(e)) => (s, e),
            _ => return None,
        };
        Some(self.slice_rows(s, e))
    }
}

/// Flatten chunks into Cursor-ordered `(cursor, chunk_idx, row_in_chunk)`.
/// Asserts KV scan order and dedups by cursor.
pub fn flatten_ordered(chunks: &[EventChunk]) -> Vec<(Cursor, u32, u32)> {
    let mut entries: Vec<(Cursor, u32, u32)> = Vec::new();
    for (ci, ch) in chunks.iter().enumerate() {
        for ri in 0..ch.num_rows() {
            entries.push((ch.cursors[ri], ci as u32, ri as u32));
        }
    }
    for w in entries.windows(2) {
        assert!(
            w[0].0 <= w[1].0,
            "EventChunks must be Cursor-ordered (KV raw scan order); got {:?} then {:?}",
            w[0].0,
            w[1].0,
        );
    }
    entries.dedup_by_key(|(c, _, _)| *c);
    entries
}

pub fn cursors_from_batch(batch: &RecordBatch, ts_column_index: usize) -> Result<Vec<Cursor>> {
    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow!("ts column"))?;
    let seq_idx = batch.schema().index_of(SEQ_NO_COLUMN_NAME)?;
    let seq = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("seq column"))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push(Cursor::new(ts.value(i), seq.value(i)));
    }
    Ok(out)
}
