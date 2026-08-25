//! Pack rows by downstream subtask; split a packed batch back into per-key groups.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, UInt8Array};
use arrow::compute;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;

use crate::common::message::{Message, KEY_FIELDS_EXTRA, TARGET_SUBTASK_EXTRA};
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

/// Split a KeyBy-packed message using `KEY_FIELDS_EXTRA` column names.
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
        let batch = message.record_batch();
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        return vec![(global_partition_key(), batch.clone())];
    }
    let batch = message.record_batch();
    let schema = batch.schema();
    let mut arrays = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in &names {
        let (idx, field) = schema
            .column_with_name(name)
            .unwrap_or_else(|| panic!("key column '{}' not in batch", name));
        arrays.push(batch.column(idx).clone());
        fields.push(field.clone());
    }
    split_by_key_arrays(batch, &arrays, fields)
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

    use crate::runtime::partition::{HashPartition, PartitionTrait};

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn pack_by_dest_p2_skips_empty_buckets_and_hash_partition_reads_extra() {
        let message = Message::new(None, int_batch(vec![10, 20, 30]), None, None);
        let hashes = [0u64, 1, 0];
        let packed = pack_by_dest(&message, &hashes, &["id".to_string()], 3);
        assert_eq!(packed.len(), 2, "dest 2 is empty and skipped");

        let mut dests = HashSet::new();
        let mut hp = HashPartition::new();
        let mut rows = 0usize;
        for m in &packed {
            let dest: usize = m
                .get_extras()
                .unwrap()
                .get(TARGET_SUBTASK_EXTRA)
                .unwrap()
                .parse()
                .unwrap();
            dests.insert(dest);
            assert_eq!(hp.partition(m, 3), vec![dest]);
            rows += m.record_batch().num_rows();
        }
        assert_eq!(rows, 3);
        assert_eq!(dests, HashSet::from([0, 1]));
    }

    #[test]
    fn split_empty_key_fields_is_global_key() {
        let message = Message::new(None, int_batch(vec![1, 2, 3]), None, None);
        let hashes = vec![0u64; 3];
        let packed = pack_by_dest(&message, &hashes, &[], 2);
        assert_eq!(packed.len(), 1);
        assert_eq!(
            packed[0].get_extras().unwrap().get(TARGET_SUBTASK_EXTRA).unwrap(),
            "0"
        );
        assert_eq!(
            packed[0].get_extras().unwrap().get(KEY_FIELDS_EXTRA).unwrap(),
            ""
        );
        let groups = split_message_by_key_fields(&packed[0]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.num_rows(), 3);
        assert_eq!(groups[0].0, global_partition_key());
    }

    #[test]
    fn split_does_not_merge_distinct_keys_with_same_hash() {
        let key_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let hashes = [7u64, 7, 7];
        let groups = group_row_indices(&hashes, &[keys], key_schema);
        assert_eq!(groups.len(), 2);
        let mut sizes: Vec<usize> = groups.iter().map(|(_, idxs)| idxs.len()).collect();
        sizes.sort();
        assert_eq!(sizes, vec![1, 2]);
    }
}
