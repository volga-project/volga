use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::kernels::boolean::and_kleene;
use arrow::compute::{filter, is_not_null, take};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::error::DataFusionError;
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDF};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::aggregates::{AggKind, AggregatorType};
use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::heap::{TopKMap, TopKOrder};
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

#[derive(Debug)]
pub(crate) struct GroupedAggTopKAccumulator {
    kind: AggKind,
    base_udaf: Arc<AggregateUDF>,
    n: Option<usize>,
    value_type: Option<DataType>,
    per_key: HashMap<ScalarValue, Box<dyn Accumulator>>,
    topk: TopKMap<ScalarValue>,
}

fn df_error(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

impl GroupedAggTopKAccumulator {
    pub(crate) fn new(kind: AggKind, base_udaf: Arc<AggregateUDF>, order: TopKOrder) -> Self {
        Self {
            kind,
            base_udaf,
            n: None,
            value_type: None,
            per_key: HashMap::new(),
            topk: TopKMap::new(order),
        }
    }

    fn ensure_value_type(&mut self, value_array: &ArrayRef) -> Result<DataType> {
        if let Some(t) = &self.value_type {
            return Ok(t.clone());
        }
        let coerced = if matches!(self.kind, AggKind::Count) {
            value_array.data_type().clone()
        } else {
            let coerced = self.base_udaf.coerce_types(&[value_array.data_type().clone()])?;
            coerced
                .get(0)
                .cloned()
                .ok_or_else(|| df_error("failed to coerce value type"))?
        };
        self.value_type = Some(coerced.clone());
        Ok(coerced)
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
        if matches!(self.kind.aggregator_type(), AggregatorType::RetractableAccumulator) {
            self.base_udaf.create_sliding_accumulator(acc_args)
        } else {
            self.base_udaf.accumulator(acc_args)
        }
    }

    fn parse_n(&mut self, n_array: &ArrayRef) -> Result<usize> {
        let value = scalar_from_array(n_array)?;
        let n = scalar_to_usize(&value)?;
        if let Some(existing) = self.n {
            if existing != n {
                return exec_err!("top n value must be constant");
            }
            return Ok(existing);
        }
        self.n = Some(n);
        Ok(n)
    }

    fn build_mask(cond_array: &BooleanArray, cate_array: &ArrayRef) -> Result<BooleanArray> {
        let not_null = is_not_null(cate_array.as_ref())?;
        and_kleene(cond_array, &not_null).map_err(|e| df_error(e.to_string()))
    }

    fn group_indices_by_key(
        cate_array: &ArrayRef,
    ) -> Result<Vec<(ScalarValue, Vec<u32>)>> {
        #[derive(Debug)]
        struct Bucket {
            key: ScalarValue,
            indices: Vec<u32>,
        }
        let mut hashes = vec![0u64; cate_array.len()];
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        create_hashes(&[cate_array.clone()], &random_state, &mut hashes)?;
        let mut by_hash: HashMap<u64, Vec<Bucket>> = HashMap::new();
        for row in 0..cate_array.len() {
            let hash = hashes[row];
            let buckets = by_hash.entry(hash).or_default();
            let mut matched = false;
            for bucket in buckets.iter_mut() {
                if bucket.key.eq_array(cate_array, row)? {
                    bucket.indices.push(row as u32);
                    matched = true;
                    break;
                }
            }
            if !matched {
                let value = ScalarValue::try_from_array(cate_array.as_ref(), row)
                    .map_err(|e| df_error(e.to_string()))?;
                buckets.push(Bucket {
                    key: value,
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

    fn update_group(
        &mut self,
        key: ScalarValue,
        value_type: &DataType,
        values: &ArrayRef,
        indices: &[u32],
        is_retract: bool,
    ) -> Result<()> {
        if !self.per_key.contains_key(&key) {
            let acc = self.build_accumulator(value_type)?;
            self.per_key.insert(key.clone(), acc);
        }
        let acc = self.per_key.get_mut(&key).expect("acc exists");
        let idx = UInt32Array::from(indices.to_vec());
        let taken = take(values.as_ref(), &idx, None)
            .map_err(|e| df_error(e.to_string()))?;
        if is_retract {
            acc.retract_batch(&[taken])?;
        } else {
            acc.update_batch(&[taken])?;
        }
        let metric = acc.evaluate()?;
        if matches!(metric, ScalarValue::Null) {
            self.per_key.remove(&key);
            self.topk.remove(&key);
        } else {
            self.topk.update_metric(key, metric);
        }
        Ok(())
    }
}

impl Accumulator for GroupedAggTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let cond_array = values
            .get(1)
            .ok_or_else(|| df_error("missing condition arg"))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| df_error("condition must be boolean"))?;
        let cate_array = values.get(2).ok_or_else(|| df_error("missing category arg"))?;
        let n_array = values.get(3).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let value_type = self.ensure_value_type(value_array)?;
        let mask = Self::build_mask(cond_array, cate_array)?;
        let filtered_values = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let filtered_cate = filter(cate_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        if filtered_cate.len() == 0 {
            return Ok(());
        }
        let grouped = Self::group_indices_by_key(&filtered_cate)?;
        for (key, indices) in grouped {
            self.update_group(key, &value_type, &filtered_values, &indices, false)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !matches!(self.kind.aggregator_type(), AggregatorType::RetractableAccumulator) {
            return exec_err!("retract not supported for {}", self.base_udaf.name());
        }
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let cond_array = values
            .get(1)
            .ok_or_else(|| df_error("missing condition arg"))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| df_error("condition must be boolean"))?;
        let cate_array = values.get(2).ok_or_else(|| df_error("missing category arg"))?;
        let n_array = values.get(3).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let value_type = self.ensure_value_type(value_array)?;
        let mask = Self::build_mask(cond_array, cate_array)?;
        let filtered_values = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let filtered_cate = filter(cate_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        if filtered_cate.len() == 0 {
            return Ok(());
        }
        let grouped = Self::group_indices_by_key(&filtered_cate)?;
        for (key, indices) in grouped {
            self.update_group(key, &value_type, &filtered_values, &indices, true)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n = self.n.unwrap_or(0);
        let pairs = self.topk.top_n(n);
        let mut parts = Vec::with_capacity(pairs.len());
        for (key, metric) in pairs {
            let key_str = match scalar_to_string(&key) {
                Some(v) => v,
                None => continue,
            };
            let metric_str = match scalar_to_string(&metric) {
                Some(v) => v,
                None => continue,
            };
            parts.push(format!("{key_str}:{metric_str}"));
        }
        Ok(ScalarValue::Utf8(Some(join_csv(&parts))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut entries: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::with_capacity(self.per_key.len());
        for (key, acc) in self.per_key.iter_mut() {
            let key_bytes = scalar_value_to_bytes(key)
                .map_err(|e| df_error(format!("state key encode failed: {e}")))?;
            let state = acc
                .state()?
                .into_iter()
                .map(|v| scalar_value_to_bytes(&v))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| df_error(format!("state encode failed: {e}")))?;
            entries.push((key_bytes, state));
        }
        let encoded = EncodedAggTopKState { entries };
        let bytes = bincode::serialize(&encoded)
            .map_err(|e| df_error(format!("state encode failed: {e}")))?;
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states.get(0).ok_or_else(|| df_error("missing state array"))?;
        let bin = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| df_error("state array must be binary"))?;
        for row in 0..bin.len() {
            if bin.is_null(row) {
                continue;
            }
            let bytes = bin.value(row);
            let decoded: EncodedAggTopKState = bincode::deserialize(bytes)
                .map_err(|e| df_error(format!("state decode failed: {e}")))?;
            for (key_bytes, state_bytes) in decoded.entries {
                let key_value = scalar_value_from_bytes(&key_bytes)
                    .map_err(|e| df_error(format!("state key decode failed: {e}")))?;
                let value_type = infer_value_type(self.kind, &state_bytes)?;
                if !self.per_key.contains_key(&key_value) {
                    let acc = self.build_accumulator(&value_type)?;
                    self.per_key.insert(key_value.clone(), acc);
                }
                let acc = self.per_key.get_mut(&key_value).expect("acc exists");
                let arrays: Vec<ArrayRef> = state_bytes
                    .iter()
                    .map(|bytes| scalar_value_from_bytes(bytes))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| df_error(format!("state decode failed: {e}")))?
                    .into_iter()
                    .map(|v| v.to_array_of_size(1))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| df_error(format!("state array convert failed: {e}")))?;
                acc.merge_batch(&arrays)?;
                let metric = acc.evaluate()?;
                if matches!(metric, ScalarValue::Null) {
                    self.per_key.remove(&key_value);
                    self.topk.remove(&key_value);
                } else {
                    self.topk.update_metric(key_value, metric);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.per_key.len() * 128
    }

    fn supports_retract_batch(&self) -> bool {
        matches!(self.kind.aggregator_type(), AggregatorType::RetractableAccumulator)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EncodedAggTopKState {
    entries: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
}

#[derive(Debug)]
pub(crate) struct RatioTopKAccumulator {
    n: Option<usize>,
    counts: HashMap<ScalarValue, RatioCounts>,
    topk: TopKMap<ScalarValue>,
}

impl RatioTopKAccumulator {
    pub(crate) fn new(order: TopKOrder) -> Self {
        Self {
            n: None,
            counts: HashMap::new(),
            topk: TopKMap::new(order),
        }
    }

    fn parse_n(&mut self, n_array: &ArrayRef) -> Result<usize> {
        let value = scalar_from_array(n_array)?;
        let n = scalar_to_usize(&value)?;
        if let Some(existing) = self.n {
            if existing != n {
                return exec_err!("top n value must be constant");
            }
            return Ok(existing);
        }
        self.n = Some(n);
        Ok(n)
    }

    fn build_total_mask(value_array: &ArrayRef, cate_array: &ArrayRef) -> Result<BooleanArray> {
        let value_not_null = is_not_null(value_array.as_ref())?;
        let cate_not_null = is_not_null(cate_array.as_ref())?;
        and_kleene(&value_not_null, &cate_not_null).map_err(|e| df_error(e.to_string()))
    }

    fn group_counts(cate_array: &ArrayRef) -> Result<Vec<(ScalarValue, usize)>> {
        group_counts_by_value(cate_array)
    }
}

impl Accumulator for RatioTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let cond_array = values
            .get(1)
            .ok_or_else(|| df_error("missing condition arg"))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| df_error("condition must be boolean"))?;
        let cate_array = values.get(2).ok_or_else(|| df_error("missing category arg"))?;
        let n_array = values.get(3).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let total_mask = Self::build_total_mask(value_array, cate_array)?;
        let total_cate = filter(cate_array.as_ref(), &total_mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&total_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.total += count as i64;
            touched.insert(key);
        }

        let match_mask = and_kleene(cond_array, &total_mask)
            .map_err(|e| df_error(e.to_string()))?;
        let match_cate = filter(cate_array.as_ref(), &match_mask)
            .map_err(|e| df_error(e.to_string()))?;
        for (key, count) in Self::group_counts(&match_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.matched += count as i64;
            touched.insert(key);
        }

        for key in touched {
            let entry = self.counts.get(&key).expect("entry exists");
            let metric = ratio_scalar(entry.matched, entry.total);
            if matches!(metric, ScalarValue::Null) {
                self.topk.remove(&key);
            } else {
                self.topk.update_metric(key, metric);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let cond_array = values
            .get(1)
            .ok_or_else(|| df_error("missing condition arg"))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| df_error("condition must be boolean"))?;
        let cate_array = values.get(2).ok_or_else(|| df_error("missing category arg"))?;
        let n_array = values.get(3).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let total_mask = Self::build_total_mask(value_array, cate_array)?;
        let total_cate = filter(cate_array.as_ref(), &total_mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&total_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.total -= count as i64;
            touched.insert(key);
        }

        let match_mask = and_kleene(cond_array, &total_mask)
            .map_err(|e| df_error(e.to_string()))?;
        let match_cate = filter(cate_array.as_ref(), &match_mask)
            .map_err(|e| df_error(e.to_string()))?;
        for (key, count) in Self::group_counts(&match_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.matched -= count as i64;
            touched.insert(key);
        }

        for key in touched {
            let entry = self.counts.get(&key).expect("entry exists");
            if entry.total <= 0 {
                self.counts.remove(&key);
                self.topk.remove(&key);
                continue;
            }
            let metric = ratio_scalar(entry.matched, entry.total);
            self.topk.update_metric(key, metric);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n = self.n.unwrap_or(0);
        let pairs = self.topk.top_n(n);
        let mut parts = Vec::with_capacity(pairs.len());
        for (key, metric) in pairs {
            let key_str = match scalar_to_string(&key) {
                Some(v) => v,
                None => continue,
            };
            let metric_str = match scalar_to_string(&metric) {
                Some(v) => v,
                None => continue,
            };
            parts.push(format!("{key_str}:{metric_str}"));
        }
        Ok(ScalarValue::Utf8(Some(join_csv(&parts))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut entries: Vec<(Vec<u8>, RatioCounts)> = Vec::with_capacity(self.counts.len());
        for (key, counts) in self.counts.iter() {
            let key_bytes = scalar_value_to_bytes(key)
                .map_err(|e| df_error(format!("state key encode failed: {e}")))?;
            entries.push((key_bytes, counts.clone()));
        }
        let encoded = EncodedRatioState { entries };
        let bytes = bincode::serialize(&encoded)
            .map_err(|e| df_error(format!("state encode failed: {e}")))?;
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states.get(0).ok_or_else(|| df_error("missing state array"))?;
        let bin = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| df_error("state array must be binary"))?;
        for row in 0..bin.len() {
            if bin.is_null(row) {
                continue;
            }
            let decoded: EncodedRatioState = bincode::deserialize(bin.value(row))
                .map_err(|e| df_error(format!("state decode failed: {e}")))?;
            for (key_bytes, counts) in decoded.entries {
                let key = scalar_value_from_bytes(&key_bytes)
                    .map_err(|e| df_error(format!("state key decode failed: {e}")))?;
                let entry = self.counts.entry(key.clone()).or_default();
                entry.total += counts.total;
                entry.matched += counts.matched;
                let metric = ratio_scalar(entry.matched, entry.total);
                if matches!(metric, ScalarValue::Null) {
                    self.topk.remove(&key);
                } else {
                    self.topk.update_metric(key, metric);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.len() * 64
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RatioCounts {
    matched: i64,
    total: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct EncodedRatioState {
    entries: Vec<(Vec<u8>, RatioCounts)>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FrequencyMode {
    TopN,
    Top1Ratio,
}

#[derive(Debug)]
pub(crate) struct FrequencyTopKAccumulator {
    mode: FrequencyMode,
    n: Option<usize>,
    total: i64,
    counts: HashMap<ScalarValue, i64>,
    topk: TopKMap<ScalarValue>,
}

impl FrequencyTopKAccumulator {
    pub(crate) fn new(mode: FrequencyMode) -> Self {
        Self {
            mode,
            n: None,
            total: 0,
            counts: HashMap::new(),
            topk: TopKMap::new(TopKOrder::MetricDesc),
        }
    }

    fn parse_n(&mut self, n_array: Option<&ArrayRef>) -> Result<Option<usize>> {
        if n_array.is_none() {
            return Ok(None);
        }
        let value = scalar_from_array(n_array.expect("n array"))?;
        let n = scalar_to_usize(&value)?;
        if let Some(existing) = self.n {
            if existing != n {
                return exec_err!("top n value must be constant");
            }
            return Ok(Some(existing));
        }
        self.n = Some(n);
        Ok(Some(n))
    }

    fn group_counts(values: &ArrayRef) -> Result<Vec<(ScalarValue, usize)>> {
        group_counts_by_value(values)
    }
}

impl Accumulator for FrequencyTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        self.parse_n(values.get(1))?;
        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&filtered)? {
            *self.counts.entry(key.clone()).or_default() += count as i64;
            self.total += count as i64;
            touched.insert(key);
        }
        for key in touched {
            let count = self.counts.get(&key).copied().unwrap_or(0);
            self.topk.update_metric(key, ScalarValue::Int64(Some(count)));
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        self.parse_n(values.get(1))?;
        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&filtered)? {
            *self.counts.entry(key.clone()).or_default() -= count as i64;
            self.total -= count as i64;
            touched.insert(key);
        }
        for key in touched {
            let count = self.counts.get(&key).copied().unwrap_or(0);
            if count <= 0 {
                self.counts.remove(&key);
                self.topk.remove(&key);
            } else {
                self.topk.update_metric(key, ScalarValue::Int64(Some(count)));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.mode {
            FrequencyMode::TopN => {
                let n = self.n.unwrap_or(0);
                let pairs = self.topk.top_n(n);
                let mut parts = Vec::with_capacity(pairs.len());
                for (key, _metric) in pairs {
                    if let Some(v) = scalar_to_string(&key) {
                        parts.push(v);
                    }
                }
                Ok(ScalarValue::Utf8(Some(join_csv(&parts))))
            }
            FrequencyMode::Top1Ratio => {
                if self.total <= 0 {
                    return Ok(ScalarValue::Float64(Some(0.0)));
                }
                let top = self.topk.top_n(1);
                if let Some((_key, metric)) = top.first() {
                    let count = scalar_to_i64(metric)?;
                    let ratio = count as f64 / self.total as f64;
                    Ok(ScalarValue::Float64(Some(ratio)))
                } else {
                    Ok(ScalarValue::Float64(Some(0.0)))
                }
            }
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut entries: Vec<(Vec<u8>, i64)> = Vec::with_capacity(self.counts.len());
        for (key, count) in self.counts.iter() {
            let key_bytes = scalar_value_to_bytes(key)
                .map_err(|e| df_error(format!("state key encode failed: {e}")))?;
            entries.push((key_bytes, *count));
        }
        let encoded = EncodedFrequencyState {
            entries,
            total: self.total,
        };
        let bytes = bincode::serialize(&encoded)
            .map_err(|e| df_error(format!("state encode failed: {e}")))?;
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states.get(0).ok_or_else(|| df_error("missing state array"))?;
        let bin = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| df_error("state array must be binary"))?;
        for row in 0..bin.len() {
            if bin.is_null(row) {
                continue;
            }
            let decoded: EncodedFrequencyState = bincode::deserialize(bin.value(row))
                .map_err(|e| df_error(format!("state decode failed: {e}")))?;
            self.total += decoded.total;
            for (key_bytes, count) in decoded.entries {
                let key = scalar_value_from_bytes(&key_bytes)
                    .map_err(|e| df_error(format!("state key decode failed: {e}")))?;
                *self.counts.entry(key.clone()).or_default() += count;
                let current = self.counts.get(&key).copied().unwrap_or(0);
                if current <= 0 {
                    self.counts.remove(&key);
                    self.topk.remove(&key);
                } else {
                    self.topk.update_metric(key, ScalarValue::Int64(Some(current)));
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.len() * 64
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EncodedFrequencyState {
    entries: Vec<(Vec<u8>, i64)>,
    total: i64,
}

fn ratio_scalar(matched: i64, total: i64) -> ScalarValue {
    if total <= 0 {
        ScalarValue::Null
    } else {
        ScalarValue::Float64(Some(matched as f64 / total as f64))
    }
}

fn scalar_from_array(array: &ArrayRef) -> Result<ScalarValue> {
    if array.len() == 0 {
        return exec_err!("empty top n array");
    }
    ScalarValue::try_from_array(array.as_ref(), 0)
        .map_err(|e| df_error(e.to_string()))
}

fn scalar_to_usize(value: &ScalarValue) -> Result<usize> {
    match value {
        ScalarValue::Int32(Some(v)) => Ok(*v as usize),
        ScalarValue::Int64(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt32(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt64(Some(v)) => Ok(*v as usize),
        _ => exec_err!("top n must be integer"),
    }
}

fn scalar_to_i64(value: &ScalarValue) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        _ => exec_err!("count must be integer"),
    }
}

fn infer_value_type(kind: AggKind, state: &[Vec<u8>]) -> Result<DataType> {
    if state.is_empty() {
        return exec_err!("empty state");
    }
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(state.len());
    for bytes in state {
        scalars.push(
            scalar_value_from_bytes(bytes).map_err(|e| df_error(e.to_string()))?,
        );
    }
    let scalar = match kind {
        AggKind::Avg => scalars.get(1).or_else(|| scalars.get(0)),
        AggKind::Sum | AggKind::Min | AggKind::Max => scalars.get(0),
        AggKind::Count => Some(&ScalarValue::Int64(Some(0))),
        _ => return exec_err!("unsupported agg kind for top state"),
    }
    .ok_or_else(|| df_error("missing state value"))?;
    Ok(scalar.data_type())
}

pub(crate) fn group_counts_by_value(values: &ArrayRef) -> Result<Vec<(ScalarValue, usize)>> {
    let grouped = GroupedAggTopKAccumulator::group_indices_by_key(values)?;
    Ok(grouped
        .into_iter()
        .map(|(k, idx)| (k, idx.len()))
        .collect())
}
