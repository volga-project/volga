use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::{filter, is_not_null, take};
use arrow::compute::kernels::boolean::and_kleene;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
    Volatility,
};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::prelude::SessionContext;
use ahash::RandomState;
use datafusion::functions_aggregate::{
    average::avg_udaf,
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use serde::{Deserialize, Serialize};

use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AggKind {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AggFlavor {
    Where,
    Cate,
    CateWhere,
}

#[derive(Debug, Clone)]
struct CateUdfSpec {
    name: String,
    kind: AggKind,
    flavor: AggFlavor,
    base_udaf: Arc<AggregateUDF>,
}

impl CateUdfSpec {
    fn new(name: &str, kind: AggKind, flavor: AggFlavor, base_udaf: Arc<AggregateUDF>) -> Self {
        Self {
            name: name.to_string(),
            kind,
            flavor,
            base_udaf,
        }
    }

    fn has_condition(&self) -> bool {
        matches!(self.flavor, AggFlavor::Where | AggFlavor::CateWhere)
    }

    fn has_category(&self) -> bool {
        matches!(self.flavor, AggFlavor::Cate | AggFlavor::CateWhere)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncodedStateBytes {
    single: Option<Vec<Vec<u8>>>,
    cate: Option<Vec<(Vec<u8>, Vec<Vec<u8>>)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CateKey {
    hash: u64,
    value: ScalarValue,
}

impl Hash for CateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the precomputed hash for fast lookup; ScalarValue is used to disambiguate collisions.
        self.hash.hash(state);
    }
}

#[derive(Debug)]
struct CateAccumulator {
    kind: AggKind,
    has_condition: bool,
    has_category: bool,
    base_udaf: Arc<AggregateUDF>,
    value_type: Option<DataType>,
    single: Option<Box<dyn Accumulator>>,
    cate: HashMap<CateKey, Box<dyn Accumulator>>,
}

impl CateAccumulator {
    fn new(spec: &CateUdfSpec, base_udaf: Arc<AggregateUDF>) -> Self {
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
        let mut grouped: HashMap<CateKey, Vec<u32>> = HashMap::new();
        for row in 0..cate_array.len() {
            let value = ScalarValue::try_from_array(cate_array.as_ref(), row)
                .map_err(|e| df_error(format!("category value decode failed: {e}")))?;
            let key = CateKey {
                hash: hashes[row],
                value,
            };
            grouped.entry(key).or_default().push(row as u32);
        }

        for (key, indices) in grouped {
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
        let mut grouped: HashMap<CateKey, Vec<u32>> = HashMap::new();
        for row in 0..cate_array.len() {
            let value = ScalarValue::try_from_array(cate_array.as_ref(), row)
                .map_err(|e| df_error(format!("category value decode failed: {e}")))?;
            let key = CateKey {
                hash: hashes[row],
                value,
            };
            grouped.entry(key).or_default().push(row as u32);
        }

        let mut to_remove: Vec<CateKey> = Vec::new();
        for (key, indices) in grouped {
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

    fn evaluate(&mut self) -> Result<datafusion::scalar::ScalarValue> {
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

    fn state(&mut self) -> Result<Vec<datafusion::scalar::ScalarValue>> {
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

#[derive(Debug, Clone)]
struct CateUdf {
    spec: CateUdfSpec,
    signature: Signature,
}

impl CateUdf {
    fn new(spec: CateUdfSpec) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            spec,
        }
    }

    fn state_fields_impl(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        let field = Field::new(
            format!("{}_state", args.name),
            DataType::Binary,
            true,
        );
        Ok(vec![Arc::new(field)])
    }
}

impl AggregateUDFImpl for CateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.spec.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.spec.has_category() {
            return Ok(DataType::Utf8);
        }
        if arg_types.is_empty() {
            return exec_err!("{} expects at least 1 argument", self.spec.name);
        }
        let coerced = coerce_value_type(self.spec.kind, self.spec.base_udaf.as_ref(), &arg_types[0])?;
        self.spec.base_udaf.return_type(&[coerced])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let expected_args = match self.spec.flavor {
            AggFlavor::Where => 2,
            AggFlavor::Cate => 2,
            AggFlavor::CateWhere => 3,
        };
        if arg_types.len() != expected_args {
            return exec_err!(
                "{} expects {} arguments, got {}",
                self.spec.name,
                expected_args,
                arg_types.len()
            );
        }

        let mut out: Vec<DataType> = Vec::with_capacity(arg_types.len());
        let coerced = coerce_value_type(self.spec.kind, self.spec.base_udaf.as_ref(), &arg_types[0])?;
        out.push(coerced);

        let mut idx = 1;
        if self.spec.has_condition() {
            out.push(DataType::Boolean);
            idx += 1;
        }
        if self.spec.has_category() {
            let cate_type = &arg_types[idx];
            let supported = matches!(
                cate_type,
                DataType::Utf8
                    | DataType::Int64
                    | DataType::Int32
                    | DataType::UInt64
                    | DataType::UInt32
                    | DataType::Float64
                    | DataType::Float32
                    | DataType::Boolean
            );
            if !supported {
                return exec_err!("{} unsupported category type {}", self.spec.name, cate_type);
            }
            out.push(cate_type.clone());
        }

        Ok(out)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CateAccumulator::new(
            &self.spec,
            self.spec.base_udaf.clone(),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        self.state_fields_impl(args)
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Err(df_error("groups accumulator not supported"))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

fn format_float(value: f64) -> String {
    let s = format!("{value:.6}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(format_float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(format_float(*v)),
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        ScalarValue::Decimal128(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        ScalarValue::Decimal256(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        _ => None,
    }
}

fn acc_state_to_bytes(acc: &mut dyn Accumulator) -> Result<Vec<Vec<u8>>> {
    let vals = acc.state()?;
    vals.into_iter()
        .map(|v| {
            scalar_value_to_bytes(&v).map_err(|e| df_error(format!("state encode failed: {e}")))
        })
        .collect()
}

fn merge_state_bytes(acc: &mut dyn Accumulator, state: &[Vec<u8>]) -> Result<()> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(state.len());
    for bytes in state {
        let v = scalar_value_from_bytes(bytes)
            .map_err(|e| df_error(format!("state decode failed: {e}")))?;
        let arr = v
            .to_array_of_size(1)
            .map_err(|e| df_error(format!("state array convert failed: {e}")))?;
        arrays.push(arr);
    }
    acc.merge_batch(&arrays)
}

fn infer_value_type(kind: AggKind, state: &[Vec<u8>]) -> Result<DataType> {
    if state.is_empty() {
        return exec_err!("empty state");
    }
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(state.len());
    for bytes in state {
        scalars.push(
            scalar_value_from_bytes(bytes)
                .map_err(|e| df_error(format!("state decode failed: {e}")))?,
        );
    }
    let scalar = match kind {
        AggKind::Avg => scalars.get(1).or_else(|| scalars.get(0)),
        AggKind::Sum | AggKind::Min | AggKind::Max => scalars.get(0),
        AggKind::Count => Some(&ScalarValue::Int64(Some(0))),
    }
    .ok_or_else(|| df_error("missing state value"))?;
    Ok(scalar.data_type())
}

fn coerce_value_type(
    kind: AggKind,
    base_udaf: &AggregateUDF,
    value_type: &DataType,
) -> Result<DataType> {
    if matches!(kind, AggKind::Count) {
        return Ok(value_type.clone());
    }
    let coerced = base_udaf.coerce_types(&[value_type.clone()])?;
    if coerced.is_empty() {
        return exec_err!("failed to coerce value type");
    }
    Ok(coerced[0].clone())
}

fn cate_udaf(spec: CateUdfSpec) -> AggregateUDF {
    AggregateUDF::from(CateUdf::new(spec))
}

fn base_udaf(kind: AggKind) -> Arc<AggregateUDF> {
    match kind {
        AggKind::Sum => sum_udaf(),
        AggKind::Avg => avg_udaf(),
        AggKind::Count => count_udaf(),
        AggKind::Min => min_udaf(),
        AggKind::Max => max_udaf(),
    }
}

fn df_error(msg: impl Into<String>) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(msg.into())
}

pub fn register_cate_udafs(ctx: &SessionContext) {
    let fns = vec![
        cate_udaf(CateUdfSpec::new(
            "sum_where",
            AggKind::Sum,
            AggFlavor::Where,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_where",
            AggKind::Avg,
            AggFlavor::Where,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_where",
            AggKind::Count,
            AggFlavor::Where,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_where",
            AggKind::Min,
            AggFlavor::Where,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_where",
            AggKind::Max,
            AggFlavor::Where,
            base_udaf(AggKind::Max),
        )),
        cate_udaf(CateUdfSpec::new(
            "sum_cate",
            AggKind::Sum,
            AggFlavor::Cate,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_cate",
            AggKind::Avg,
            AggFlavor::Cate,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_cate",
            AggKind::Count,
            AggFlavor::Cate,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_cate",
            AggKind::Min,
            AggFlavor::Cate,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_cate",
            AggKind::Max,
            AggFlavor::Cate,
            base_udaf(AggKind::Max),
        )),
        cate_udaf(CateUdfSpec::new(
            "sum_cate_where",
            AggKind::Sum,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_cate_where",
            AggKind::Avg,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_cate_where",
            AggKind::Count,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_cate_where",
            AggKind::Min,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_cate_where",
            AggKind::Max,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Max),
        )),
    ];
    for f in fns {
        ctx.register_udaf(f);
    }
}

#[cfg(test)]
mod tests;
