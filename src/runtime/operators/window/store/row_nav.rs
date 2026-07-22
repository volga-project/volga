//! In-memory cursor-ordered row list for RANGE window evaluation.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::event_store::StoredRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdx(pub usize);

#[derive(Debug)]
pub struct RowNav {
    rows: Arc<[StoredRow]>,
    /// Precomputed args per row (same length as rows).
    args: Vec<Option<Arc<Vec<ArrayRef>>>>,
}

impl RowNav {
    pub fn from_stored_with_args(
        rows: impl Into<Arc<[StoredRow]>>,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> Self {
        let rows = rows.into();
        let args: Vec<_> = rows
            .iter()
            .map(|r| {
                Some(Arc::new(
                    window_expr
                        .evaluate_args(&r.batch)
                        .expect("evaluate_args"),
                ))
            })
            .collect();
        Self { rows, args }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn first_idx(&self) -> Option<RowIdx> {
        if self.rows.is_empty() {
            None
        } else {
            Some(RowIdx(0))
        }
    }

    pub fn last_idx(&self) -> Option<RowIdx> {
        if self.rows.is_empty() {
            None
        } else {
            Some(RowIdx(self.rows.len() - 1))
        }
    }

    pub fn cursor(&self, idx: RowIdx) -> Cursor {
        self.rows[idx.0].cursor
    }

    pub fn batch(&self, idx: RowIdx) -> &RecordBatch {
        &self.rows[idx.0].batch
    }

    pub fn args(&self, idx: RowIdx) -> Option<&Arc<Vec<ArrayRef>>> {
        self.args[idx.0].as_ref()
    }

    pub fn next(&self, idx: RowIdx) -> Option<RowIdx> {
        let n = idx.0 + 1;
        if n < self.rows.len() {
            Some(RowIdx(n))
        } else {
            None
        }
    }

    /// First row with cursor > prev (or first row if prev is None).
    pub fn first_update_idx(&self, prev: Option<Cursor>) -> Option<RowIdx> {
        let prev = prev.unwrap_or(Cursor::new(i64::MIN, 0));
        self.rows
            .iter()
            .position(|r| r.cursor > prev)
            .map(RowIdx)
    }

    /// Last row with cursor <= target.
    pub fn seek_le(&self, target: Cursor) -> Option<RowIdx> {
        match self.rows.iter().rposition(|r| r.cursor <= target) {
            Some(i) => Some(RowIdx(i)),
            None => None,
        }
    }

    /// First index with cursor >= target.
    pub fn seek_ge(&self, target: Cursor) -> Option<RowIdx> {
        self.rows
            .iter()
            .position(|r| r.cursor >= target)
            .map(RowIdx)
    }

    /// First index with ts >= start_ts (for RANGE window start).
    pub fn seek_ts_ge(&self, start_ts: i64) -> Option<RowIdx> {
        self.rows
            .iter()
            .position(|r| r.cursor.ts >= start_ts)
            .map(RowIdx)
    }
}
