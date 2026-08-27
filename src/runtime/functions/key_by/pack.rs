//! Pack rows by downstream subtask; split a packed batch back into per-key groups.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, UInt8Array};
use arrow::compute;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;

use crate::common::key_group::subtask_for_hash;
use crate::common::message::{Message, TARGET_SUBTASK_EXTRA};
use crate::common::Key;

fn take_rows(batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
    let indices_array = UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices_array, None).expect("take rows"))
        .collect();
    RecordBatch::try_new(batch.schema(), columns).expect("taken batch")
}

fn global_partition_key() -> Key {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_volga_global",
        arrow::datatypes::DataType::UInt8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt8Array::from(vec![0]))])
        .expect("global key batch");
    Key::new(batch).expect("global key")
}

pub(crate) fn pack_by_dest(
    message: &Message,
    hashes: &[u64],
    parallelism: usize,
    max_parallelism: usize,
) -> Vec<Message> {
    let batch = message.record_batch();
    assert_eq!(
        hashes.len(),
        batch.num_rows(),
        "one hash per row"
    );
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); parallelism];
    for (i, &hash) in hashes.iter().enumerate() {
        buckets[subtask_for_hash(hash, parallelism, max_parallelism)].push(i);
    }
    let mut out = Vec::new();
    for (dest, idxs) in buckets.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }
        let packed = take_rows(batch, &idxs);
        let mut extras = message.get_extras().unwrap_or_default();
        extras.insert(TARGET_SUBTASK_EXTRA.to_string(), dest.to_string());
        out.push(Message::new(
            message.upstream_vertex_id(),
            packed,
            message.ingest_timestamp(),
            Some(extras),
        ));
    }
    out
}

fn take_key_row(key_arrays: &[ArrayRef], key_schema: &Arc<Schema>, row: usize) -> RecordBatch {
    let idx = UInt32Array::from(vec![row as u32]);
    let key_cols: Vec<ArrayRef> = key_arrays
        .iter()
        .map(|array| compute::take(array.as_ref(), &idx, None).expect("key row"))
        .collect();
    RecordBatch::try_new(key_schema.clone(), key_cols).expect("key batch")
}

/// Group rows by hash, then split colliding hashes with the same `Key::eq`
/// record-batch compare so distinct keys are not merged.
fn group_row_indices(
    hashes: &[u64],
    key_arrays: &[ArrayRef],
    key_schema: Arc<Schema>,
) -> Vec<(Key, Vec<usize>)> {
    let mut by_hash: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, &hash) in hashes.iter().enumerate() {
        by_hash.entry(hash).or_default().push(i);
    }
    let mut out = Vec::with_capacity(by_hash.len());
    for (hash, idxs) in by_hash {
        let mut subgroups: Vec<(RecordBatch, Vec<usize>)> = Vec::new();
        for i in idxs {
            let row = take_key_row(key_arrays, &key_schema, i);
            if let Some((_, members)) = subgroups.iter_mut().find(|(k, _)| k == &row) {
                members.push(i);
            } else {
                subgroups.push((row, vec![i]));
            }
        }
        for (key_batch, members) in subgroups {
            out.push((Key::with_hash(key_batch, hash).expect("key"), members));
        }
    }
    out
}

fn split_by_key_arrays(
    batch: &RecordBatch,
    key_arrays: &[ArrayRef],
    key_fields: Vec<Field>,
) -> Vec<(Key, RecordBatch)> {
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let hashes = Key::hash_arrays(key_arrays, batch.num_rows()).expect("key hashes");
    let groups = group_row_indices(&hashes, key_arrays, Arc::new(Schema::new(key_fields)));
    groups
        .into_iter()
        .map(|(key, idxs)| (key, take_rows(batch, &idxs)))
        .collect()
}

pub fn split_by_key_exprs(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
) -> Vec<(Key, RecordBatch)> {
    if exprs.is_empty() {
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        return vec![(global_partition_key(), batch.clone())];
    }
    let arrays = evaluate_key_arrays(exprs, batch);
    let fields = key_fields_for_exprs(exprs, &arrays, batch.schema().as_ref());
    split_by_key_arrays(batch, &arrays, fields)
}

pub(crate) fn evaluate_key_arrays(
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

pub(crate) fn key_fields_for_exprs(
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn pack_by_dest_skips_empty_buckets() {
        let message = Message::new(None, int_batch(vec![10, 20, 30]), None, None);
        let packed = pack_by_dest(&message, &[0u64, 1, 0], 3, 3);
        assert_eq!(packed.len(), 2, "dest 2 is empty and skipped");

        let mut dests = HashSet::new();
        let mut rows = 0usize;
        for m in &packed {
            let extras = m.get_extras().unwrap();
            dests.insert(extras.get(TARGET_SUBTASK_EXTRA).unwrap().clone());
            rows += m.record_batch().num_rows();
        }
        assert_eq!(rows, 3);
        assert_eq!(dests, HashSet::from(["0".to_string(), "1".to_string()]));
    }

    #[test]
    fn split_does_not_merge_distinct_keys_with_same_hash() {
        let key_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let groups = group_row_indices(&[7u64, 7, 7], &[keys], key_schema);
        assert_eq!(groups.len(), 2);
        let mut sizes: Vec<usize> = groups.iter().map(|(_, idxs)| idxs.len()).collect();
        sizes.sort();
        assert_eq!(sizes, vec![1, 2]);
    }
}
