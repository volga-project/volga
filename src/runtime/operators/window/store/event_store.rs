//! Raw event SortedKV IO: one row per key, bucketed for envelope scans.
//!
//! ```text
//! raw/{ns}/{key_hash}/{bucket_ts}/{ts}/{seq_no} → single-row Arrow IPC
//! ```
//!
//! `bucket_ts` is for load planning (scan only buckets overlapping the eval
//! envelope). Writes are one put per row — no multi-row batch packing / `seg_id`.
//!
//! TODO(perf): optional multi-row packing inside a bucket —
//! https://github.com/volga-project/volga/issues/155

use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::StreamExt;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::store::event_chunk::{
    cursors_from_batch, EventChunk,
};
use crate::storage::{KvSnapshot, SortedKV, WriteBatch};

use super::keys::{
    parse_bucket_ts_from_raw_key, parse_cursor_from_raw_key, raw_cursor_key, raw_prefix,
    raw_scan_end_exclusive, raw_scan_start, StateNamespace,
};
use super::row_codec::{decode_batch, encode_batch};

#[derive(Debug, Clone)]
pub struct EventStore {
    kv: Arc<dyn SortedKV>,
    ns: StateNamespace,
}

impl EventStore {
    pub fn new(kv: Arc<dyn SortedKV>, ns: StateNamespace) -> Self {
        Self { kv, ns }
    }

    /// Append each row as its own KV entry under `align_bucket(ts)`.
    /// Batch must already include `__seq_no`.
    pub fn append_batch(
        &self,
        wb: &mut WriteBatch,
        key: &Key,
        batch: &RecordBatch,
        ts_column_index: usize,
        bucket_ms: i64,
    ) -> Result<Vec<Cursor>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }
        let cursors = cursors_from_batch(batch, ts_column_index)?;
        for (i, c) in cursors.iter().enumerate() {
            let row = batch.slice(i, 1);
            let bytes = encode_batch(&row)?;
            wb.put(raw_cursor_key(&self.ns, key, *c, bucket_ms), bytes);
        }
        Ok(cursors)
    }

    /// Inclusive cursor scan over buckets overlapping `[from, to]`.
    ///
    /// Pass a [`KvSnapshot`] from `snapshot_partition` for multi-op loads.
    pub async fn scan(
        &self,
        reader: &dyn KvSnapshot,
        key: &Key,
        from: Cursor,
        to: Cursor,
        bucket_ms: i64,
    ) -> Result<Vec<EventChunk>> {
        if to < from {
            return Ok(vec![]);
        }
        let start = raw_scan_start(&self.ns, key, from, bucket_ms);
        let end = raw_scan_end_exclusive(&self.ns, key, to, bucket_ms);

        let mut out = Vec::new();
        let mut stream = reader.scan(&start, &end);
        while let Some(item) = stream.next().await {
            let (_k, v) = item?;
            let batch = decode_batch(&v)?;
            let ts_idx = timestamp_col_index(&batch)?;
            let chunk = EventChunk::from_batch(batch, ts_idx)?;
            if let Some(sliced) = chunk.slice_inclusive(from, to) {
                out.push(sliced);
            }
        }
        Ok(out)
    }

    /// Delete raw rows with `cursor < cutoff` (drop whole buckets when possible).
    pub async fn prune_before(
        &self,
        key: &Key,
        cutoff: Cursor,
        bucket_ms: i64,
    ) -> Result<()> {
        let start = raw_prefix(&self.ns, key);
        let mut end = start.clone();
        end.push(0xff);
        let mut stream = self.kv.scan(&start, &end);
        let mut wb = WriteBatch::new();
        let b = bucket_ms.max(1);

        while let Some(item) = stream.next().await {
            let (k, _v) = item?;
            if let Some(bucket_ts) = parse_bucket_ts_from_raw_key(&k) {
                if bucket_ts.saturating_add(b) <= cutoff.ts {
                    wb.delete(k);
                    continue;
                }
            }
            let Some(c) = parse_cursor_from_raw_key(&k) else {
                wb.delete(k);
                continue;
            };
            if c < cutoff {
                wb.delete(k);
            }
        }

        if !wb.is_empty() {
            self.kv.write(wb).await?;
        }
        Ok(())
    }
}

fn timestamp_col_index(batch: &RecordBatch) -> Result<usize> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| {
            matches!(
                f.data_type(),
                arrow::datatypes::DataType::Timestamp(_, _)
            )
        })
        .ok_or_else(|| anyhow::anyhow!("no timestamp column"))
}
