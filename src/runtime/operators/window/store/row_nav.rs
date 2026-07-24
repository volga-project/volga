//! In-memory cursor-ordered navigation over [`EventChunk`]s.
//!
//! Callers must pass chunks already in [`Cursor`] order (SortedKV scan over
//! `raw/…/{bucket_ts}/{ts}/{seq}`). We assert that and only dedup.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::event_chunk::{flatten_ordered, EventChunk};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdx(pub usize);

#[derive(Debug)]
pub struct RowNav {
    chunks: Arc<[EventChunk]>,
    /// Flat index: global row → (chunk, row-in-chunk).
    locs: Vec<(u32, u32)>,
    cursors: Vec<Cursor>,
    /// `evaluate_args` once per chunk (full-length arrays).
    chunk_args: Vec<Option<Vec<ArrayRef>>>,
}

impl RowNav {
    pub fn from_chunks(
        chunks: impl Into<Arc<[EventChunk]>>,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> Self {
        let chunks = chunks.into();
        let entries = flatten_ordered(&chunks);
        let mut locs = Vec::with_capacity(entries.len());
        let mut cursors = Vec::with_capacity(entries.len());
        for (c, ci, ri) in entries {
            locs.push((ci, ri));
            cursors.push(c);
        }

        let chunk_args: Vec<_> = chunks
            .iter()
            .map(|ch| {
                if ch.is_empty() {
                    None
                } else {
                    Some(
                        window_expr
                            .evaluate_args(ch.batch.as_ref())
                            .expect("evaluate_args"),
                    )
                }
            })
            .collect();

        Self {
            chunks,
            locs,
            cursors,
            chunk_args,
        }
    }

    pub fn cursor(&self, idx: RowIdx) -> Cursor {
        self.cursors[idx.0]
    }

    /// 1-row Arrow slice for this global row (zero-copy buffers).
    pub fn batch(&self, idx: RowIdx) -> RecordBatch {
        let (ci, ri) = self.locs[idx.0];
        self.chunks[ci as usize]
            .batch
            .slice(ri as usize, 1)
    }

    /// Per-row args: 1-row slices from the chunk-level `evaluate_args`.
    pub fn args(&self, idx: RowIdx) -> Option<Vec<ArrayRef>> {
        let (ci, ri) = self.locs[idx.0];
        let cols = self.chunk_args[ci as usize].as_ref()?;
        Some(cols.iter().map(|a| a.slice(ri as usize, 1)).collect())
    }

    pub fn next(&self, idx: RowIdx) -> Option<RowIdx> {
        let n = idx.0 + 1;
        if n < self.locs.len() {
            Some(RowIdx(n))
        } else {
            None
        }
    }

    /// First row with cursor > prev (or first row if prev is None).
    pub fn first_update_idx(&self, prev: Option<Cursor>) -> Option<RowIdx> {
        let prev = prev.unwrap_or(Cursor::new(i64::MIN, 0));
        self.cursors
            .iter()
            .position(|c| *c > prev)
            .map(RowIdx)
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
        self.cursors
            .iter()
            .position(|c| *c >= target)
            .map(RowIdx)
    }

    /// First index with ts >= start_ts (for RANGE window start).
    pub fn seek_ts_ge(&self, start_ts: i64) -> Option<RowIdx> {
        self.cursors
            .iter()
            .position(|c| c.ts >= start_ts)
            .map(RowIdx)
    }
}
