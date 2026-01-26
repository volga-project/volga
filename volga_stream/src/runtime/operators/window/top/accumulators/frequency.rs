use std::collections::HashMap;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{filter, is_not_null};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::heap::{TopKMap, TopKOrder};
use crate::runtime::operators::window::top::utils::group_counts_by_value;
use crate::runtime::operators::window::top::accumulators::common::{
    df_error, parse_n_optional,
};
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

pub(crate) const TOPN_FREQUENCY_NAME: &str = "topn_frequency";
pub(crate) const TOP1_RATIO_NAME: &str = "top1_ratio";

#[derive(Debug, Clone, Copy)]
pub(crate) enum FrequencyMode {
    TopN,
    Top1Ratio,
}

#[derive(Debug)]
// For topn_frequency and top1_ratio: per-value frequency with top-k or ratio output.
pub(crate) struct FrequencyTopKAccumulator {
    mode: FrequencyMode,
    n: Option<usize>,
    total: i64,
    counts: HashMap<ScalarValue, i64>,
    topk: TopKMap<ScalarValue>,
}

impl FrequencyTopKAccumulator {
    pub(crate) fn new(mode: FrequencyMode) -> Self {
        let order = TopKOrder::MetricDesc;
        Self {
            mode,
            n: None,
            total: 0,
            counts: HashMap::new(),
            topk: TopKMap::new(order),
        }
    }

    fn parse_n(&mut self, n_array: Option<&ArrayRef>) -> Result<Option<usize>> {
        parse_n_optional(&mut self.n, n_array)
    }

    fn apply_counts(&mut self, key: ScalarValue, delta: i64) {
        let entry = self.counts.entry(key.clone()).or_default();
        *entry += delta;
        if *entry <= 0 {
            self.counts.remove(&key);
            self.topk.remove(&key);
            return;
        }
        self.topk
            .update_metric(key, ScalarValue::Int64(Some(*entry)));
    }
}

impl Accumulator for FrequencyTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        self.parse_n(values.get(1))?;
        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        self.total += filtered.len() as i64;
        for (key, count) in group_counts_by_value(&filtered)? {
            self.apply_counts(key, count as i64);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        self.parse_n(values.get(1))?;
        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        self.total -= filtered.len() as i64;
        for (key, count) in group_counts_by_value(&filtered)? {
            self.apply_counts(key, -(count as i64));
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
            total: self.total,
            entries,
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
                self.apply_counts(key, count);
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
    total: i64,
    entries: Vec<(Vec<u8>, i64)>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Int64Array, StringArray};

    fn string_array(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn int_array(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn test_frequency_topn_update_retract() {
        let mut acc = FrequencyTopKAccumulator::new(FrequencyMode::TopN);
        let values = string_array(&["a", "b", "a", "a", "c"]);
        let n = int_array(&[2, 2, 2, 2, 2]);
        acc.update_batch(&[values.clone(), n.clone()]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("a,c".to_string()))
        );

        let retract_values = string_array(&["c"]);
        let retract_n = int_array(&[2]);
        acc.retract_batch(&[retract_values, retract_n]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("a,b".to_string()))
        );
    }

    #[test]
    fn test_frequency_top1_ratio() {
        let mut acc = FrequencyTopKAccumulator::new(FrequencyMode::Top1Ratio);
        let values = string_array(&["a", "b", "a", "c"]);
        acc.update_batch(&[values.clone()]).unwrap();
        let out = acc.evaluate().unwrap();
        assert_eq!(out, ScalarValue::Float64(Some(0.5)));
    }

    #[test]
    fn test_frequency_merge() {
        let mut left = FrequencyTopKAccumulator::new(FrequencyMode::TopN);
        let left_values = string_array(&["a"]);
        let left_n = int_array(&[2]);
        left.update_batch(&[left_values, left_n]).unwrap();

        let mut right = FrequencyTopKAccumulator::new(FrequencyMode::TopN);
        let right_values = string_array(&["a", "b"]);
        let right_n = int_array(&[2, 2]);
        right.update_batch(&[right_values, right_n]).unwrap();

        let state = right.state().unwrap();
        let state_arrays = state
            .into_iter()
            .map(|s| s.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();
        left.merge_batch(&state_arrays).unwrap();

        assert_eq!(
            left.evaluate().unwrap(),
            ScalarValue::Utf8(Some("a,b".to_string()))
        );
    }
}
