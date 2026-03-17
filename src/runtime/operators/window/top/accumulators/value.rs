use arrow::array::{Array, ArrayRef};
use arrow::compute::{filter, is_not_null};
use datafusion::common::Result;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::top::accumulators::common::{df_error, parse_n_once};
use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::heap::{TopKMap, TopKOrder};
use crate::runtime::operators::window::top::utils::group_counts_by_value;
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

pub(crate) const TOP_NAME: &str = "top";

#[derive(Debug)]
// For top(value, k): multiset of values with duplicates preserved.
pub(crate) struct TopValueAccumulator {
    n: Option<usize>,
    topk: TopKMap<ScalarValue>,
}

impl TopValueAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            n: None,
            topk: TopKMap::new(TopKOrder::KeyDesc),
        }
    }

    fn parse_n(&mut self, n_array: &ArrayRef) -> Result<usize> {
        parse_n_once(&mut self.n, n_array)
    }

    fn current_count(&self, value: &ScalarValue) -> i64 {
        self.topk
            .get_metric(value)
            .and_then(scalar_to_i64)
            .unwrap_or(0)
    }

    fn update_count(&mut self, value: ScalarValue, count: i64) {
        if count <= 0 {
            self.topk.remove(&value);
        } else {
            self.topk
                .update_metric(value, ScalarValue::Int64(Some(count)));
        }
    }
}

impl Accumulator for TopValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let n_array = values.get(1).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        for (value, count) in group_counts_by_value(&filtered)? {
            let new_count = self.current_count(&value) + count as i64;
            self.update_count(value.clone(), new_count);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let n_array = values.get(1).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask).map_err(|e| df_error(e.to_string()))?;
        for (value, count) in group_counts_by_value(&filtered)? {
            let new_count = self.current_count(&value) - count as i64;
            self.update_count(value.clone(), new_count);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n = self.n.unwrap_or(0);
        if n == 0 {
            return Ok(ScalarValue::Utf8(Some(String::new())));
        }
        let mut parts: Vec<String> = Vec::new();
        let entries = self.topk.top_n(n);
        for (value, metric) in entries {
            let count = metric_as_i64(&metric).unwrap_or(0);
            if count <= 0 {
                continue;
            }
            let value_str = scalar_to_string(&value).unwrap_or_default();
            let take = (n - parts.len()).min(count as usize);
            for _ in 0..take {
                parts.push(value_str.clone());
            }
            if parts.len() >= n {
                break;
            }
        }
        Ok(ScalarValue::Utf8(Some(join_csv(&parts))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let snapshot = self.topk.entries();
        let mut entries: Vec<(Vec<u8>, i64)> = Vec::with_capacity(snapshot.len());
        for (value, metric) in snapshot {
            let Some(count) = metric_as_i64(&metric) else {
                continue;
            };
            let value_bytes = scalar_value_to_bytes(&value)
                .map_err(|e| df_error(format!("state encode failed: {e}")))?;
            entries.push((value_bytes, count));
        }
        let encoded = EncodedTopState { entries };
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
            let decoded: EncodedTopState = bincode::deserialize(bin.value(row))
                .map_err(|e| df_error(format!("state decode failed: {e}")))?;
            for (value_bytes, count) in decoded.entries {
                let value = scalar_value_from_bytes(&value_bytes)
                    .map_err(|e| df_error(format!("state decode failed: {e}")))?;
                let current = self.current_count(&value);
                self.update_count(value, current + count);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.topk.len() * 32
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EncodedTopState {
    entries: Vec<(Vec<u8>, i64)>,
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        _ => None,
    }
}

fn metric_as_i64(value: &ScalarValue) -> Option<i64> {
    scalar_to_i64(value)
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
    fn test_top_value_update_retract() {
        let mut acc = TopValueAccumulator::new();
        let values = string_array(&["b", "a", "a", "c"]);
        let n = int_array(&[3, 3, 3, 3]);
        acc.update_batch(&[values.clone(), n.clone()]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("c,b,a".to_string()))
        );

        let retract_values = string_array(&["c"]);
        let retract_n = int_array(&[3]);
        acc.retract_batch(&[retract_values, retract_n]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("b,a,a".to_string()))
        );
    }
}
