//! Raw event SortedKV IO: scan / append / prune. No fan-out joins.

use anyhow::Result;
use arrow::array::{Array, RecordBatch, TimestampMillisecondArray, UInt64Array};
use futures::StreamExt;
use std::sync::Arc;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::storage::{SortedKV, WriteBatch};

use super::keys::{raw_key, raw_prefix, raw_scan_end, raw_scan_start, StateNamespace};
use super::row_codec::{decode_batch, encode_row};

#[derive(Debug, Clone)]
pub struct StoredRow {
    pub cursor: Cursor,
    pub batch: RecordBatch, // 1-row
}

#[derive(Debug, Clone)]
pub struct EventStore {
    kv: Arc<dyn SortedKV>,
    ns: StateNamespace,
}

impl EventStore {
    pub fn new(kv: Arc<dyn SortedKV>, ns: StateNamespace) -> Self {
        Self { kv, ns }
    }

    /// Append encoded row puts into `wb` (does not commit).
    pub fn append_batch_rows(
        &self,
        wb: &mut WriteBatch,
        key: &Key,
        batch: &RecordBatch,
        ts_column_index: usize,
    ) -> Result<Vec<Cursor>> {
        let ts = batch
            .column(ts_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts column");
        let seq_idx = batch
            .schema()
            .index_of(SEQ_NO_COLUMN_NAME)
            .expect("__seq_no");
        let seq = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("seq column");

        let mut cursors = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let cursor = Cursor::new(ts.value(i), seq.value(i));
            let bytes = encode_row(batch, i)?;
            wb.put(raw_key(&self.ns, key, cursor), bytes);
            cursors.push(cursor);
        }
        Ok(cursors)
    }

    /// Inclusive scan: rows with `from <= cursor <= to`. One KV scan.
    pub async fn scan(
        &self,
        key: &Key,
        from: Cursor,
        to: Cursor,
    ) -> Result<Vec<StoredRow>> {
        if to < from {
            return Ok(vec![]);
        }
        let start = raw_scan_start(&self.ns, key, from);
        let end = if to.seq_no == u64::MAX {
            raw_scan_end(&self.ns, key, to.ts.saturating_add(1))
        } else {
            raw_key(
                &self.ns,
                key,
                Cursor::new(to.ts, to.seq_no.saturating_add(1)),
            )
        };

        let mut out = Vec::new();
        let mut stream = self.kv.scan(&start, &end);
        while let Some(item) = stream.next().await {
            let (_k, v) = item?;
            let batch = decode_batch(&v)?;
            let cursor = cursor_from_row(&batch, 0)?;
            if cursor < from || cursor > to {
                continue;
            }
            out.push(StoredRow { cursor, batch });
        }
        Ok(out)
    }

    /// List + batch-delete raw keys with cursor < cutoff.
    pub async fn prune_before(&self, key: &Key, cutoff: Cursor) -> Result<()> {
        let start = raw_prefix(&self.ns, key);
        let end = raw_key(&self.ns, key, cutoff);
        let mut to_delete = Vec::new();
        let mut stream = self.kv.scan(&start, &end);
        while let Some(item) = stream.next().await {
            let (k, _) = item?;
            to_delete.push(k);
        }
        if to_delete.is_empty() {
            return Ok(());
        }
        let mut wb = WriteBatch::with_capacity(to_delete.len());
        for k in to_delete {
            wb.delete(k);
        }
        self.kv.write(wb).await
    }
}

fn cursor_from_row(batch: &RecordBatch, row: usize) -> Result<Cursor> {
    let ts_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| {
            matches!(
                f.data_type(),
                arrow::datatypes::DataType::Timestamp(_, _)
            )
        })
        .ok_or_else(|| anyhow::anyhow!("no timestamp column"))?;
    let ts = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow::anyhow!("ts not TimestampMillisecond"))?
        .value(row);
    let seq_idx = batch.schema().index_of(SEQ_NO_COLUMN_NAME)?;
    let seq = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow::anyhow!("seq not u64"))?
        .value(row);
    Ok(Cursor::new(ts, seq))
}
