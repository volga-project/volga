use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::kernels::boolean::and_kleene;
use arrow::compute::{filter, is_not_null, take};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDF};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::aggregates::{AggKind, AggregatorType};
use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::heap::{TopKMap, TopKOrder};
use crate::runtime::operators::window::top::utils::group_indices_by_scalar;
use crate::runtime::operators::window::top::accumulators::common::{
    df_error, infer_value_type, parse_n_once,
};
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

#[derive(Debug)]
// For top_n_key/value_*_cate_where: per-key aggregate + top-k ordering.
pub(crate) struct GroupedAggTopKAccumulator {
    kind: AggKind,
    base_udaf: Arc<AggregateUDF>,
    n: Option<usize>,
    value_type: Option<DataType>,
    per_key: HashMap<ScalarValue, Box<dyn Accumulator>>,
    topk: TopKMap<ScalarValue>,
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
        parse_n_once(&mut self.n, n_array)
    }

    fn build_mask(cond_array: &BooleanArray, cate_array: &ArrayRef) -> Result<BooleanArray> {
        let not_null = is_not_null(cate_array.as_ref())?;
        and_kleene(cond_array, &not_null).map_err(|e| df_error(e.to_string()))
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
        let taken = take(values.as_ref(), &idx, None).map_err(|e| df_error(e.to_string()))?;
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
        let filtered_values = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        let filtered_cate = filter(cate_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        if filtered_cate.len() == 0 {
            return Ok(());
        }
        let grouped = group_indices_by_scalar(&filtered_cate)?;
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
        let filtered_values = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        let filtered_cate = filter(cate_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        if filtered_cate.len() == 0 {
            return Ok(());
        }
        let grouped = group_indices_by_scalar(&filtered_cate)?;
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
            let key_bytes =
                scalar_value_to_bytes(key).map_err(|e| df_error(format!("state key encode failed: {e}")))?;
            let state = acc
                .state()?
                .into_iter()
                .map(|s| scalar_value_to_bytes(&s).map_err(|e| df_error(format!("state encode failed: {e}"))))
                .collect::<Result<Vec<_>>>()?;
            entries.push((key_bytes, state));
        }
        let encoded = EncodedAggTopKState { entries };
        let bytes = bincode::serialize(&encoded).map_err(|e| df_error(format!("state encode failed: {e}")))?;
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
            let decoded: EncodedAggTopKState =
                bincode::deserialize(bin.value(row)).map_err(|e| df_error(format!("state decode failed: {e}")))?;
            for (key_bytes, state_bytes) in decoded.entries {
                let key = scalar_value_from_bytes(&key_bytes)
                    .map_err(|e| df_error(format!("state key decode failed: {e}")))?;
                let value_type = infer_value_type(self.kind, &state_bytes)?;
                if !self.per_key.contains_key(&key) {
                    let acc = self.build_accumulator(&value_type)?;
                    self.per_key.insert(key.clone(), acc);
                }
                let acc = self.per_key.get_mut(&key).expect("acc exists");
                let arrays: Vec<ArrayRef> = state_bytes
                    .iter()
                    .map(|bytes| {
                        let v = scalar_value_from_bytes(bytes)
                            .map_err(|e| df_error(format!("state decode failed: {e}")))?;
                        v.to_array_of_size(1)
                            .map_err(|e| df_error(format!("state array convert failed: {e}")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                acc.merge_batch(&arrays)?;
                let metric = acc.evaluate()?;
                if matches!(metric, ScalarValue::Null) {
                    self.per_key.remove(&key);
                    self.topk.remove(&key);
                } else {
                    self.topk.update_metric(key, metric);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::functions_aggregate::sum::sum_udaf;

    fn value_array(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn bool_array(values: &[bool]) -> ArrayRef {
        Arc::new(BooleanArray::from(values.to_vec()))
    }

    fn string_array(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn int_array(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn test_grouped_topk_update_retract() {
        let mut acc = GroupedAggTopKAccumulator::new(AggKind::Sum, sum_udaf(), TopKOrder::MetricDesc);
        let values = value_array(&[1.0, 2.0, 3.0, 4.0]);
        let conds = bool_array(&[true, true, true, true]);
        let cates = string_array(&["a", "b", "a", "c"]);
        let n = int_array(&[2, 2, 2, 2]);
        acc.update_batch(&[values.clone(), conds.clone(), cates.clone(), n.clone()])
            .unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("c:4,a:4".to_string()))
        );

        let retract_values = value_array(&[4.0]);
        let retract_conds = bool_array(&[true]);
        let retract_cates = string_array(&["c"]);
        let retract_n = int_array(&[2]);
        acc.retract_batch(&[retract_values, retract_conds, retract_cates, retract_n])
            .unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("a:4,b:2".to_string()))
        );
    }

    #[test]
    fn test_grouped_topk_merge() {
        let mut left = GroupedAggTopKAccumulator::new(AggKind::Sum, sum_udaf(), TopKOrder::MetricDesc);
        let left_values = value_array(&[1.0]);
        let left_conds = bool_array(&[true]);
        let left_cates = string_array(&["a"]);
        let left_n = int_array(&[2]);
        left.update_batch(&[left_values, left_conds, left_cates, left_n])
            .unwrap();

        let mut right =
            GroupedAggTopKAccumulator::new(AggKind::Sum, sum_udaf(), TopKOrder::MetricDesc);
        let right_values = value_array(&[2.0, 3.0]);
        let right_conds = bool_array(&[true, true]);
        let right_cates = string_array(&["a", "b"]);
        let right_n = int_array(&[2, 2]);
        right
            .update_batch(&[right_values, right_conds, right_cates, right_n])
            .unwrap();

        let state = right.state().unwrap();
        let state_arrays = state
            .into_iter()
            .map(|s| s.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();
        left.merge_batch(&state_arrays).unwrap();

        assert_eq!(
            left.evaluate().unwrap(),
            ScalarValue::Utf8(Some("b:3,a:3".to_string()))
        );
    }
}
