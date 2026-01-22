use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::top::grouped_topk::{FrequencyMode, FrequencyTopKAccumulator};
use crate::runtime::operators::window::top::value_topk::TopValueAccumulator;

#[test]
fn test_top_value_update_retract() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 2.0]));
    let n: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2]));
    let mut acc = TopValueAccumulator::new();
    acc.update_batch(&[values.clone(), n.clone()])
        .expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("2,2".to_string())));

    let retract_values: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
    let retract_n: ArrayRef = Arc::new(Int64Array::from(vec![2]));
    acc.retract_batch(&[retract_values, retract_n])
        .expect("retract");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("2,1".to_string())));
}

#[test]
fn test_top_value_merge() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 3.0, 3.0]));
    let n: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2]));
    let mut left = TopValueAccumulator::new();
    left.update_batch(&[values.clone(), n.clone()])
        .expect("update");

    let mut right = TopValueAccumulator::new();
    let state = left.state().expect("state");
    let state_array = state[0].to_array_of_size(1).expect("state array");
    right.merge_batch(&[state_array]).expect("merge");
    let out = right.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("3,3".to_string())));
}

#[test]
fn test_topn_frequency_update_retract() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 1.0, 2.0]));
    let n: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2]));
    let mut acc = FrequencyTopKAccumulator::new(FrequencyMode::TopN);
    acc.update_batch(&[values.clone(), n.clone()])
        .expect("update");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("1,2".to_string())));

    let retract_values: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    let retract_n: ArrayRef = Arc::new(Int64Array::from(vec![2]));
    acc.retract_batch(&[retract_values, retract_n])
        .expect("retract");
    let out = acc.evaluate().expect("evaluate");
    assert_eq!(out, ScalarValue::Utf8(Some("2,1".to_string())));
}
