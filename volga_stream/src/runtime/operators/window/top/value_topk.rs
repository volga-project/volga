use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use arrow::array::{Array, ArrayRef};
use arrow::compute::{filter, is_not_null};
use datafusion::common::{exec_err, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::top::format::{join_csv, scalar_to_string};
use crate::runtime::operators::window::top::grouped_topk::group_counts_by_value;
use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

#[derive(Debug)]
pub(crate) struct TopValueAccumulator {
    n: Option<usize>,
    counts: HashMap<ScalarValue, i64>,
    heap: BinaryHeap<ValueEntry>,
}

fn df_error(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

impl TopValueAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            n: None,
            counts: HashMap::new(),
            heap: BinaryHeap::new(),
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

    fn push_entry(&mut self, value: ScalarValue, count: i64) {
        self.heap.push(ValueEntry { value, count });
    }
}

impl Accumulator for TopValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let n_array = values.get(1).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (value, count) in group_counts_by_value(&filtered)? {
            *self.counts.entry(value.clone()).or_default() += count as i64;
            touched.insert(value);
        }
        for value in touched {
            let count = self.counts.get(&value).copied().unwrap_or(0);
            if count > 0 {
                self.push_entry(value, count);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value_array = values.get(0).ok_or_else(|| df_error("missing value arg"))?;
        let n_array = values.get(1).ok_or_else(|| df_error("missing n arg"))?;
        self.parse_n(n_array)?;

        let mask = is_not_null(value_array.as_ref()).map_err(|e| df_error(e.to_string()))?;
        let filtered = filter(value_array.as_ref(), &mask)
            .map_err(|e| df_error(e.to_string()))?;
        let mut touched: HashSet<ScalarValue> = HashSet::new();
        for (value, count) in group_counts_by_value(&filtered)? {
            *self.counts.entry(value.clone()).or_default() -= count as i64;
            touched.insert(value);
        }
        for value in touched {
            let count = self.counts.get(&value).copied().unwrap_or(0);
            if count <= 0 {
                self.counts.remove(&value);
            } else {
                self.push_entry(value, count);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n = self.n.unwrap_or(0);
        if n == 0 {
            return Ok(ScalarValue::Utf8(Some(String::new())));
        }
        let mut parts: Vec<String> = Vec::new();
        let mut kept: Vec<ValueEntry> = Vec::new();
        while parts.len() < n {
            let entry = match self.heap.pop() {
                Some(entry) => entry,
                None => break,
            };
            let current = self.counts.get(&entry.value).copied().unwrap_or(0);
            if current == entry.count && current > 0 {
                let value_str = scalar_to_string(&entry.value).unwrap_or_default();
                let take = (n - parts.len()).min(current as usize);
                for _ in 0..take {
                    parts.push(value_str.clone());
                }
                kept.push(entry);
            }
        }
        for entry in kept {
            self.heap.push(entry);
        }
        Ok(ScalarValue::Utf8(Some(join_csv(&parts))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut entries: Vec<(Vec<u8>, i64)> = Vec::with_capacity(self.counts.len());
        for (value, count) in self.counts.iter() {
            let value_bytes = scalar_value_to_bytes(value)
                .map_err(|e| df_error(format!("state encode failed: {e}")))?;
            entries.push((value_bytes, *count));
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
                *self.counts.entry(value.clone()).or_default() += count;
                let current = self.counts.get(&value).copied().unwrap_or(0);
                if current > 0 {
                    self.push_entry(value, current);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.len() * 32
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct ValueEntry {
    value: ScalarValue,
    count: i64,
}

impl PartialEq for ValueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.count == other.count
    }
}

impl Eq for ValueEntry {}

impl PartialOrd for ValueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_scalar(&self.value, &other.value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EncodedTopState {
    entries: Vec<(Vec<u8>, i64)>,
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

fn cmp_scalar(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match left.partial_cmp(right) {
        Some(ordering) => ordering,
        None => {
            let left_str = scalar_to_string(left).unwrap_or_default();
            let right_str = scalar_to_string(right).unwrap_or_default();
            left_str.cmp(&right_str)
        }
    }
}
