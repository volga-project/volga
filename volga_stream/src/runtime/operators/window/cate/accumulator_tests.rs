use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, StringArray};
use arrow::array::ArrayRef;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;

use super::accumulator::CateAccumulator;
use super::types::{AggFlavor, AggKind, CateUdfSpec};

fn value_array(values: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

fn bool_array(values: &[bool]) -> ArrayRef {
    Arc::new(BooleanArray::from(values.to_vec()))
}

fn string_array(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
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
