use std::collections::HashMap;

use ahash::RandomState;
use arrow::array::ArrayRef;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::Result;
use datafusion::scalar::ScalarValue;

pub(crate) fn group_indices_by_scalar(
    values: &ArrayRef,
) -> Result<Vec<(ScalarValue, Vec<u32>)>> {
    let grouped = group_indices_by_scalar_with_hash(values)?;
    Ok(grouped
        .into_iter()
        .map(|(value, _hash, indices)| (value, indices))
        .collect())
}

pub(crate) fn group_indices_by_scalar_with_hash(
    values: &ArrayRef,
) -> Result<Vec<(ScalarValue, u64, Vec<u32>)>> {
    #[derive(Debug)]
    struct Bucket {
        value: ScalarValue,
        indices: Vec<u32>,
    }

    let mut hashes = vec![0u64; values.len()];
    let random_state = RandomState::with_seeds(0, 0, 0, 0);
    create_hashes(&[values.clone()], &random_state, &mut hashes)?;
    let mut by_hash: HashMap<u64, Vec<Bucket>> = HashMap::new();
    for row in 0..values.len() {
        let hash = hashes[row];
        let buckets = by_hash.entry(hash).or_default();
        let mut matched = false;
        for bucket in buckets.iter_mut() {
            if bucket.value.eq_array(values, row)? {
                bucket.indices.push(row as u32);
                matched = true;
                break;
            }
        }
        if !matched {
            let value = ScalarValue::try_from_array(values.as_ref(), row)?;
            buckets.push(Bucket {
                value,
                indices: vec![row as u32],
            });
        }
    }

    let mut out = Vec::new();
    for (hash, buckets) in by_hash.into_iter() {
        for bucket in buckets {
            out.push((bucket.value, hash, bucket.indices));
        }
    }
    Ok(out)
}

pub(crate) fn group_counts_by_value(values: &ArrayRef) -> Result<Vec<(ScalarValue, usize)>> {
    let grouped = group_indices_by_scalar(values)?;
    Ok(grouped
        .into_iter()
        .map(|(value, indices)| (value, indices.len()))
        .collect())
}

pub(crate) fn hash_scalar_value(value: &ScalarValue) -> Result<u64> {
    let array = value.to_array_of_size(1)?;
    let mut hashes = vec![0u64; 1];
    let random_state = RandomState::with_seeds(0, 0, 0, 0);
    create_hashes(&[array], &random_state, &mut hashes)?;
    Ok(hashes[0])
}
