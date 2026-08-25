//! Pack rows by downstream subtask; split a packed batch back into per-key groups.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;

use crate::common::message::{Message, KEY_FIELDS_EXTRA, TARGET_SUBTASK_EXTRA};
use crate::common::Key;

pub fn take_rows(batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
    let indices_array = UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices_array, None).expect("take rows"))
        .collect();
    RecordBatch::try_new(batch.schema(), columns).expect("taken batch")
}

pub fn pack_by_dest(
    message: &Message,
    hashes: &[u64],
    key_field_names: &[String],
    num_partitions: usize,
) -> Vec<Message> {
    let batch = message.record_batch();
    assert_eq!(
        hashes.len(),
        batch.num_rows(),
        "one hash per row"
    );
    let n = num_partitions.max(1);
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, &hash) in hashes.iter().enumerate() {
        buckets[(hash % n as u64) as usize].push(i);
    }
    let key_fields_joined = key_field_names.join(",");
    let mut out = Vec::new();
    for (dest, idxs) in buckets.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }
        let packed = take_rows(batch, &idxs);
        let mut extras = message.get_extras().unwrap_or_default();
        extras.insert(TARGET_SUBTASK_EXTRA.to_string(), dest.to_string());
        extras.insert(KEY_FIELDS_EXTRA.to_string(), key_fields_joined.clone());
        out.push(Message::new(
            message.upstream_vertex_id(),
            packed,
            message.ingest_timestamp(),
            Some(extras),
        ));
    }
    out
}

pub fn split_by_key_arrays(
    batch: &RecordBatch,
    key_arrays: &[ArrayRef],
    key_fields: Vec<Field>,
) -> Vec<(Key, RecordBatch)> {
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let hashes = Key::hash_arrays(key_arrays, batch.num_rows()).expect("key hashes");
    let mut groups: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, &hash) in hashes.iter().enumerate() {
        groups.entry(hash).or_default().push(i);
    }
    let key_schema = Arc::new(Schema::new(key_fields));
    let mut out = Vec::with_capacity(groups.len());
    for (hash, idxs) in groups {
        let payload = take_rows(batch, &idxs);
        let first = idxs[0] as u32;
        let first_idx = UInt32Array::from(vec![first]);
        let key_cols: Vec<ArrayRef> = key_arrays
            .iter()
            .map(|array| compute::take(array.as_ref(), &first_idx, None).expect("key row"))
            .collect();
        let key_batch =
            RecordBatch::try_new(key_schema.clone(), key_cols).expect("key batch");
        let key = Key::with_hash(key_batch, hash).expect("key");
        out.push((key, payload));
    }
    out
}

pub fn split_by_key_column_names(
    batch: &RecordBatch,
    key_column_names: &[String],
) -> Vec<(Key, RecordBatch)> {
    if key_column_names.is_empty() {
        panic!("split_by_key_column_names requires key field names");
    }
    let schema = batch.schema();
    let mut arrays = Vec::with_capacity(key_column_names.len());
    let mut fields = Vec::with_capacity(key_column_names.len());
    for name in key_column_names {
        let (idx, field) = schema
            .column_with_name(name)
            .unwrap_or_else(|| panic!("key column '{}' not in batch", name));
        arrays.push(batch.column(idx).clone());
        fields.push(field.clone());
    }
    split_by_key_arrays(batch, &arrays, fields)
}

pub fn split_by_key_exprs(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
) -> Vec<(Key, RecordBatch)> {
    if exprs.is_empty() {
        panic!("split_by_key_exprs requires key expressions");
    }
    let arrays = evaluate_key_arrays(exprs, batch);
    let fields = key_fields_for_exprs(exprs, &arrays, batch.schema().as_ref());
    split_by_key_arrays(batch, &arrays, fields)
}

pub fn split_message_by_key_fields(message: &Message) -> Vec<(Key, RecordBatch)> {
    let names = message
        .get_extras()
        .as_ref()
        .and_then(|e| e.get(KEY_FIELDS_EXTRA))
        .map(|s| {
            s.split(',')
                .filter(|p| !p.is_empty())
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if names.is_empty() {
        panic!(
            "packed keyed hop missing {}; KeyBy must set it",
            KEY_FIELDS_EXTRA
        );
    }
    split_by_key_column_names(message.record_batch(), &names)
}

/// Drop KeyBy routing extras so downstream hops (Aggregate emit, etc.) do not
/// look like packed keyed traffic.
pub fn without_pack_extras(
    extras: Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let mut extras = extras?;
    extras.remove(TARGET_SUBTASK_EXTRA);
    extras.remove(KEY_FIELDS_EXTRA);
    if extras.is_empty() {
        None
    } else {
        Some(extras)
    }
}

pub fn evaluate_key_arrays(
    exprs: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Vec<ArrayRef> {
    exprs
        .iter()
        .map(|expr| {
            let value = expr
                .evaluate(batch)
                .expect("evaluate key expression");
            value
                .into_array(batch.num_rows())
                .expect("key array")
        })
        .collect()
}

pub fn key_fields_for_exprs(
    exprs: &[Arc<dyn PhysicalExpr>],
    arrays: &[ArrayRef],
    batch_schema: &arrow::datatypes::Schema,
) -> Vec<Field> {
    use datafusion::physical_plan::expressions::Column;

    exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                if let Some((_, field)) = batch_schema.column_with_name(col.name()) {
                    return field.clone();
                }
                if col.index() < batch_schema.fields().len() {
                    return batch_schema.field(col.index()).clone();
                }
            }
            Field::new(
                format!("_key_{i}"),
                arrays[i].data_type().clone(),
                true,
            )
        })
        .collect()
}
