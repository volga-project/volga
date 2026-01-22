use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::array::ArrayRef;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;

use super::accumulator::CateAccumulator;
use super::types::{AggFlavor, AggKind, CateUdfSpec};
use crate::runtime::operators::window::top::grouped_topk::{
    GroupedAggTopKAccumulator, RatioTopKAccumulator,
};
use crate::runtime::operators::window::top::heap::TopKOrder;

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

fn new_accumulator(flavor: AggFlavor) -> CateAccumulator {
    let spec = CateUdfSpec::new("sum_where", AggKind::Sum, flavor, sum_udaf());
    CateAccumulator::new(&spec, sum_udaf())
}

#[test]
fn test_sum_where_update_retract() {
    let mut acc = new_accumulator(AggFlavor::Where);
    let values = value_array(&[1.0, 3.0, 2.0]);
    let conds = bool_array(&[false, true, true]);
    acc.update_batch(&[values.clone(), conds.clone()]).unwrap();
    assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(5.0)));

    let retract_values = value_array(&[3.0]);
    let retract_conds = bool_array(&[true]);
    acc.retract_batch(&[retract_values, retract_conds]).unwrap();
    assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(2.0)));
}

#[test]
fn test_sum_cate_where_update_retract() {
    let mut acc = new_accumulator(AggFlavor::CateWhere);
    let values = value_array(&[1.0, 3.0, 4.0]);
    let conds = bool_array(&[false, true, true]);
    let cates = string_array(&["a", "b", "a"]);
    acc.update_batch(&[values.clone(), conds.clone(), cates.clone()])
        .unwrap();
    assert_eq!(
        acc.evaluate().unwrap(),
        ScalarValue::Utf8(Some("a:4,b:3".to_string()))
    );

    let retract_values = value_array(&[4.0]);
    let retract_conds = bool_array(&[true]);
    let retract_cates = string_array(&["a"]);
    acc.retract_batch(&[retract_values, retract_conds, retract_cates])
        .unwrap();
    assert_eq!(
        acc.evaluate().unwrap(),
        ScalarValue::Utf8(Some("b:3".to_string()))
    );
}

#[test]
fn test_sum_cate_where_merge() {
    let mut acc_left = new_accumulator(AggFlavor::CateWhere);
    let left_values = value_array(&[1.0]);
    let left_conds = bool_array(&[true]);
    let left_cates = string_array(&["a"]);
    acc_left
        .update_batch(&[left_values, left_conds, left_cates])
        .unwrap();

    let mut acc_right = new_accumulator(AggFlavor::CateWhere);
    let right_values = value_array(&[2.0, 3.0]);
    let right_conds = bool_array(&[true, true]);
    let right_cates = string_array(&["a", "b"]);
    acc_right
        .update_batch(&[right_values, right_conds, right_cates])
        .unwrap();

    let state = acc_right.state().unwrap();
    let state_arrays = state
        .into_iter()
        .map(|s| s.to_array_of_size(1).unwrap())
        .collect::<Vec<_>>();
    acc_left.merge_batch(&state_arrays).unwrap();

    assert_eq!(
        acc_left.evaluate().unwrap(),
        ScalarValue::Utf8(Some("a:3,b:3".to_string()))
    );
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

#[test]
fn test_ratio_topk_update_retract() {
    let mut acc = RatioTopKAccumulator::new(TopKOrder::KeyDesc);
    let values = value_array(&[1.0, 1.0, 1.0, 1.0]);
    let conds = bool_array(&[true, false, true, false]);
    let cates = string_array(&["a", "b", "b", "c"]);
    let n = int_array(&[2, 2, 2, 2]);
    acc.update_batch(&[values.clone(), conds.clone(), cates.clone(), n.clone()])
        .unwrap();
    assert_eq!(
        acc.evaluate().unwrap(),
        ScalarValue::Utf8(Some("c:0,b:0.5".to_string()))
    );

    let retract_values = value_array(&[1.0]);
    let retract_conds = bool_array(&[false]);
    let retract_cates = string_array(&["c"]);
    let retract_n = int_array(&[2]);
    acc.retract_batch(&[retract_values, retract_conds, retract_cates, retract_n])
        .unwrap();
    assert_eq!(
        acc.evaluate().unwrap(),
        ScalarValue::Utf8(Some("b:0.5,a:1".to_string()))
    );
}

#[test]
fn test_ratio_topk_merge() {
    let mut left = RatioTopKAccumulator::new(TopKOrder::KeyDesc);
    let left_values = value_array(&[1.0]);
    let left_conds = bool_array(&[true]);
    let left_cates = string_array(&["a"]);
    let left_n = int_array(&[2]);
    left.update_batch(&[left_values, left_conds, left_cates, left_n])
        .unwrap();

    let mut right = RatioTopKAccumulator::new(TopKOrder::KeyDesc);
    let right_values = value_array(&[1.0, 1.0]);
    let right_conds = bool_array(&[false, true]);
    let right_cates = string_array(&["b", "b"]);
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
        ScalarValue::Utf8(Some("b:0.5,a:1".to_string()))
    );
}
