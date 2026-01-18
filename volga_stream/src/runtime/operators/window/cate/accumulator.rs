use std::collections::HashMap;
use std::mem::size_of_val;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::kernels::boolean::and_kleene;
use arrow::compute::{filter, is_not_null, take};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDF};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

use super::types::{AggKind, CateKey, CateUdfSpec, EncodedStateBytes};
use super::utils::{
    acc_state_to_bytes, coerce_value_type, df_error, infer_value_type, merge_state_bytes,
    scalar_to_string,
};
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

#[derive(Debug)]
pub(crate) struct CateAccumulator {
    kind: AggKind,
    has_condition: bool,
    has_category: bool,
    base_udaf: Arc<AggregateUDF>,
    value_type: Option<DataType>,
    single: Option<Box<dyn Accumulator>>,
    cate: HashMap<CateKey, Box<dyn Accumulator>>,
}

impl CateAccumulator {
    pub(crate) fn new(spec: &CateUdfSpec, base_udaf: Arc<AggregateUDF>) -> Self {
        Self {
            kind: spec.kind,
            has_condition: spec.has_condition(),
            has_category: spec.has_category(),
            base_udaf,
            value_type: None,
            single: None,
            cate: HashMap::new(),
        }
    }

    fn ensure_value_type(&mut self, value_array: &ArrayRef) -> Result<DataType> {
        if let Some(t) = &self.value_type {
            return Ok(t.clone());
        }
        let coerced = coerce_value_type(self.kind, self.base_udaf.as_ref(), value_array.data_type())?;
        self.value_type = Some(coerced.clone());
        Ok(coerced)
    }

    fn is_retractable(kind: AggKind) -> bool {
        matches!(kind, AggKind::Sum | AggKind::Avg | AggKind::Count)
    }

    fn build_accumulator(&self, value_type: &DataType) -> Result<Box<dyn Accumulator>> {
        let coerced = if matches!(self.kind, AggKind::Count) {
            vec![value_type.clone()]
        } else {
            self.base_udaf.coerce_types(&[value_type.clone()])?
        };
        let return_type = self.base_udaf.return_type(&coerced)?;
        let input_field = Field::new("value", coerced[0].clone(), true);
        let schema = Schema::new(vec![input_field.clone()]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];
        let return_field = Arc::new(Field::new("out", return_type, true));
        let acc_args = AccumulatorArgs {
            return_field,
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.base_udaf.name(),
            is_distinct: false,
            exprs: &exprs,
        };
        if Self::is_retractable(self.kind) {
            self.base_udaf.create_sliding_accumulator(acc_args)
        } else {
            self.base_udaf.accumulator(acc_args)
        }
    }

    fn build_mask(
        cond_array: Option<&BooleanArray>,
        cate_array: Option<&ArrayRef>,
    ) -> Result<Option<BooleanArray>> {
        let mut mask = cond_array.cloned();
        if let Some(cate_array) = cate_array {
            let not_null = is_not_null(cate_array.as_ref())?;
            mask = Some(if let Some(existing) = mask {
                and_kleene(&existing, &not_null)?
            } else {
                not_null
            });
        }
        Ok(mask)
    }

    fn hash_categories(cate_array: &ArrayRef) -> Result<Vec<u64>> {
        let mut hashes = vec![0u64; cate_array.len()];
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        create_hashes(&[cate_array.clone()], &random_state, &mut hashes)?;
        Ok(hashes)
    }

    fn group_indices_by_key(
        cate_array: &ArrayRef,
        hashes: &[u64],
    ) -> Result<Vec<(CateKey, Vec<u32>)>> {
        #[derive(Debug)]
        struct Bucket {
            key: CateKey,
            indices: Vec<u32>,
        }

        let mut by_hash: HashMap<u64, Vec<Bucket>> = HashMap::new();
        for row in 0..cate_array.len() {
            let hash = hashes[row];
            let buckets = by_hash.entry(hash).or_default();
            let mut matched = false;
            for bucket in buckets.iter_mut() {
                if bucket.key.value.eq_array(cate_array, row)? {
                    bucket.indices.push(row as u32);
                    matched = true;
                    break;
                }
            }
            if !matched {
                let value = ScalarValue::try_from_array(cate_array.as_ref(), row)
                    .map_err(|e| df_error(format!("category value decode failed: {e}")))?;
                let key = CateKey { hash, value };
                buckets.push(Bucket {
                    key,
                    indices: vec![row as u32],
                });
            }
        }

        let mut out = Vec::new();
        for buckets in by_hash.into_values() {
            for bucket in buckets {
                out.push((bucket.key, bucket.indices));
            }
        }
        Ok(out)
    }

    fn key_from_value(value: ScalarValue) -> Result<CateKey> {
        let array = value
            .to_array_of_size(1)
            .map_err(|e| df_error(format!("hash value to array failed: {e}")))?;
        let hashes = Self::hash_categories(&array)?;
        Ok(CateKey {
            hash: hashes[0],
            value,
        })
    }
}

impl Accumulator for CateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values
            .get(0)
            .ok_or_else(|| df_error("missing value arg"))?;
        let value_type = self.ensure_value_type(value_array)?;
        let cond_array = if self.has_condition {
            Some(
                values
                    .get(1)
                    .ok_or_else(|| df_error("missing condition arg"))?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| df_error("condition must be boolean"))?,
            )
        } else {
            None
        };
        let cate_array = if self.has_category {
            let idx = if self.has_condition { 2 } else { 1 };
            Some(values.get(idx).ok_or_else(|| df_error("missing category arg"))?)
        } else {
            None
        };

        if !self.has_category {
            if self.single.is_none() {
                self.single = Some(self.build_accumulator(&value_type)?);
            }
            let acc = self.single.as_mut().expect("single accumulator exists");
            if let Some(mask) = Self::build_mask(cond_array, None)? {
                let filtered = filter(value_array.as_ref(), &mask)
                    .map_err(|e| df_error(format!("filter values failed: {e}")))?;
                if filtered.len() == 0 {
                    return Ok(());
                }
                acc.update_batch(&[filtered])?;
            } else {
                acc.update_batch(&[value_array.clone()])?;
            }
            return Ok(());
        }

        let cate_array = cate_array.expect("category array must exist");
        let mask = Self::build_mask(cond_array, Some(cate_array))?;
        let (value_array, cate_array) = if let Some(mask) = mask {
            let filtered_values = filter(value_array.as_ref(), &mask)
                .map_err(|e| df_error(format!("filter values failed: {e}")))?;
            let filtered_cates = filter(cate_array.as_ref(), &mask)
                .map_err(|e| df_error(format!("filter categories failed: {e}")))?;
            (filtered_values, filtered_cates)
        } else {
            (value_array.clone(), cate_array.clone())
        };

        if value_array.len() == 0 {
            return Ok(());
        }

        let hashes = Self::hash_categories(&cate_array)?;
        for (key, indices) in Self::group_indices_by_key(&cate_array, &hashes)? {
            if !self.cate.contains_key(&key) {
                let acc = self.build_accumulator(&value_type)?;
                self.cate.insert(key.clone(), acc);
            }
            let acc = self.cate.get_mut(&key).expect("acc exists");
            let indices = UInt32Array::from(indices);
            let group_values = take(value_array.as_ref(), &indices, None)
                .map_err(|e| df_error(format!("take values failed: {e}")))?;
            acc.update_batch(&[group_values])?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !Self::is_retractable(self.kind) {
            return exec_err!("retract not supported for {}", self.base_udaf.name());
        }

        let value_array = values
            .get(0)
            .ok_or_else(|| df_error("missing value arg"))?;
        let value_type = self.ensure_value_type(value_array)?;
        let cond_array = if self.has_condition {
            Some(
                values
                    .get(1)
                    .ok_or_else(|| df_error("missing condition arg"))?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| df_error("condition must be boolean"))?,
            )
        } else {
            None
        };
        let cate_array = if self.has_category {
            let idx = if self.has_condition { 2 } else { 1 };
            Some(values.get(idx).ok_or_else(|| df_error("missing category arg"))?)
        } else {
            None
        };

        if !self.has_category {
            if self.single.is_none() {
                self.single = Some(self.build_accumulator(&value_type)?);
            }
            let acc = self.single.as_mut().expect("single accumulator exists");
            if let Some(mask) = Self::build_mask(cond_array, None)? {
                let filtered = filter(value_array.as_ref(), &mask)
                    .map_err(|e| df_error(format!("filter values failed: {e}")))?;
                if filtered.len() == 0 {
                    return Ok(());
                }
                acc.retract_batch(&[filtered])?;
            } else {
                acc.retract_batch(&[value_array.clone()])?;
            }
            return Ok(());
        }

        let cate_array = cate_array.expect("category array must exist");
        let mask = Self::build_mask(cond_array, Some(cate_array))?;
        let (value_array, cate_array) = if let Some(mask) = mask {
            let filtered_values = filter(value_array.as_ref(), &mask)
                .map_err(|e| df_error(format!("filter values failed: {e}")))?;
            let filtered_cates = filter(cate_array.as_ref(), &mask)
                .map_err(|e| df_error(format!("filter categories failed: {e}")))?;
            (filtered_values, filtered_cates)
        } else {
            (value_array.clone(), cate_array.clone())
        };

        if value_array.len() == 0 {
            return Ok(());
        }

        let hashes = Self::hash_categories(&cate_array)?;
        let mut to_remove: Vec<CateKey> = Vec::new();
        for (key, indices) in Self::group_indices_by_key(&cate_array, &hashes)? {
            if !self.cate.contains_key(&key) {
                let acc = self.build_accumulator(&value_type)?;
                self.cate.insert(key.clone(), acc);
            }
            let acc = self.cate.get_mut(&key).expect("acc exists");
            let indices = UInt32Array::from(indices);
            let group_values = take(value_array.as_ref(), &indices, None)
                .map_err(|e| df_error(format!("take values failed: {e}")))?;
            acc.retract_batch(&[group_values])?;
            let empty = acc.state()?.iter().all(|s| s.is_null());
            if empty {
                to_remove.push(key);
            }
        }
        for k in to_remove {
            self.cate.remove(&k);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if !self.has_category {
            let acc = self.single.as_mut().ok_or_else(|| df_error("missing accumulator"))?;
            return acc.evaluate();
        }

        if self.cate.is_empty() {
            return Ok(ScalarValue::Utf8(Some(String::new())));
        }
        let mut parts: Vec<String> = Vec::with_capacity(self.cate.len());
        for (k, acc) in self.cate.iter_mut() {
            let cate_str = match scalar_to_string(&k.value) {
                Some(s) => s,
                None => continue,
            };
            let val = acc.evaluate()?;
            if let Some(s) = scalar_to_string(&val) {
                parts.push(format!("{cate_str}:{s}"));
            }
        }
        parts.sort();
        Ok(ScalarValue::Utf8(Some(parts.join(","))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let encoded = if !self.has_category {
            let single = if let Some(acc) = self.single.as_mut() {
                Some(acc_state_to_bytes(acc.as_mut())?)
            } else {
                None
            };
            EncodedStateBytes {
                single,
                cate: None,
            }
        } else {
            let mut cate: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::with_capacity(self.cate.len());
            for (k, acc) in self.cate.iter_mut() {
                let key_bytes = scalar_value_to_bytes(&k.value)
                    .map_err(|e| df_error(format!("state key encode failed: {e}")))?;
                cate.push((key_bytes, acc_state_to_bytes(acc.as_mut())?));
            }
            EncodedStateBytes {
                single: None,
                cate: Some(cate),
            }
        };
        let bytes = bincode::serialize(&encoded)
            .map_err(|e| df_error(format!("state encode failed: {e}")))?;
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states
            .get(0)
            .ok_or_else(|| df_error("missing state array"))?;
        let bin = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| df_error("state array must be binary"))?;

        for row in 0..bin.len() {
            if bin.is_null(row) {
                continue;
            }
            let bytes = bin.value(row);
            let decoded: EncodedStateBytes = bincode::deserialize(bytes)
                .map_err(|e| df_error(format!("state decode failed: {e}")))?;
            if let Some(single) = decoded.single {
                let value_type = infer_value_type(self.kind, &single)?;
                if self.single.is_none() {
                    self.single = Some(self.build_accumulator(&value_type)?);
                }
                let acc = self.single.as_mut().expect("single accumulator exists");
                merge_state_bytes(acc.as_mut(), &single)?;
            }
            if let Some(cate) = decoded.cate {
                for (key_bytes, state_bytes) in cate {
                    let value_type = infer_value_type(self.kind, &state_bytes)?;
                    let key_value = scalar_value_from_bytes(&key_bytes)
                        .map_err(|e| df_error(format!("state key decode failed: {e}")))?;
                    let key = Self::key_from_value(key_value)?;
                    if !self.cate.contains_key(&key) {
                        let acc = self.build_accumulator(&value_type)?;
                        self.cate.insert(key.clone(), acc);
                    }
                    let acc = self.cate.get_mut(&key).expect("acc exists");
                    merge_state_bytes(acc.as_mut(), &state_bytes)?;
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let base = size_of_val(self);
        base + self.cate.len() * 128
    }

    fn supports_retract_batch(&self) -> bool {
        Self::is_retractable(self.kind)
    }
}
