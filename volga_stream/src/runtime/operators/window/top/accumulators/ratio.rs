use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::boolean::and_kleene;
use arrow::compute::{filter, is_not_null};
use datafusion::common::Result;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::heap::{TopKMap, TopKOrder};
use crate::runtime::operators::window::top::utils::group_counts_by_value;
use crate::runtime::operators::window::top::accumulators::common::{
    df_error, parse_n_once, ratio_scalar,
};
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

pub(crate) const TOP_N_KEY_RATIO_CATE_NAME: &str = "top_n_key_ratio_cate";
pub(crate) const TOP_N_VALUE_RATIO_CATE_NAME: &str = "top_n_value_ratio_cate";

#[derive(Debug)]
// For top_n_key/value_ratio_cate: per-key matched/total ratio + top-k ordering.
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
        parse_n_once(&mut self.n, n_array)
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
        let total_cate = filter(cate_array.as_ref(), &total_mask).map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&total_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.total += count as i64;
            touched.insert(key);
        }

        let match_mask = and_kleene(cond_array, &total_mask).map_err(|e| df_error(e.to_string()))?;
        let match_cate = filter(cate_array.as_ref(), &match_mask).map_err(|e| df_error(e.to_string()))?;
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
        let total_cate = filter(cate_array.as_ref(), &total_mask).map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (key, count) in Self::group_counts(&total_cate)? {
            let entry = self.counts.entry(key.clone()).or_default();
            entry.total -= count as i64;
            touched.insert(key);
        }

        let match_mask = and_kleene(cond_array, &total_mask).map_err(|e| df_error(e.to_string()))?;
        let match_cate = filter(cate_array.as_ref(), &match_mask).map_err(|e| df_error(e.to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};

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
    fn test_ratio_topk_update_retract() {
        let mut acc = RatioTopKAccumulator::new(TopKOrder::MetricDesc);
        let values = string_array(&["x", "y", "z", "w"]);
        let conds = bool_array(&[true, false, true, true]);
        let cates = string_array(&["a", "a", "b", "b"]);
        let n = int_array(&[2, 2, 2, 2]);
        acc.update_batch(&[values.clone(), conds.clone(), cates.clone(), n.clone()])
            .unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("b:1,a:0.5".to_string()))
        );

        let retract_values = string_array(&["w"]);
        let retract_conds = bool_array(&[true]);
        let retract_cates = string_array(&["b"]);
        let retract_n = int_array(&[2]);
        acc.retract_batch(&[retract_values, retract_conds, retract_cates, retract_n])
            .unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("b:1,a:0.5".to_string()))
        );
    }

    #[test]
    fn test_ratio_topk_merge() {
        let mut left = RatioTopKAccumulator::new(TopKOrder::MetricDesc);
        let left_values = string_array(&["x"]);
        let left_conds = bool_array(&[true]);
        let left_cates = string_array(&["a"]);
        let left_n = int_array(&[2]);
        left.update_batch(&[left_values, left_conds, left_cates, left_n]).unwrap();

        let mut right = RatioTopKAccumulator::new(TopKOrder::MetricDesc);
        let right_values = string_array(&["y", "z"]);
        let right_conds = bool_array(&[false, true]);
        let right_cates = string_array(&["a", "b"]);
        let right_n = int_array(&[2, 2]);
        right.update_batch(&[right_values, right_conds, right_cates, right_n]).unwrap();

        let state = right.state().unwrap();
        let state_arrays = state
            .into_iter()
            .map(|s| s.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();
        left.merge_batch(&state_arrays).unwrap();

        assert_eq!(
            left.evaluate().unwrap(),
            ScalarValue::Utf8(Some("b:1,a:0.5".to_string()))
        );
    }
}
