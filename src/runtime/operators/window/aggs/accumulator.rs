use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::Accumulator;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::model::AccumulatorState;
use crate::runtime::utils::scalar_value_from_bytes;

use super::registry::{create_window_accumulator, AccumulatorType, AggKind};

pub(crate) fn ensure_value_type(
    value_type: &mut Option<DataType>,
    kind: AggKind,
    base_udaf: &AggregateUDF,
    value_array: &ArrayRef,
) -> Result<DataType> {
    if let Some(value_type) = value_type {
        return Ok(value_type.clone());
    }
    let coerced = coerce_value_type(kind, base_udaf, value_array.data_type())?;
    *value_type = Some(coerced.clone());
    Ok(coerced)
}

pub(crate) fn coerce_value_type(
    kind: AggKind,
    base_udaf: &AggregateUDF,
    value_type: &DataType,
) -> Result<DataType> {
    if matches!(kind, AggKind::Count) {
        return Ok(value_type.clone());
    }
    base_udaf
        .coerce_types(&[value_type.clone()])?
        .into_iter()
        .next()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("failed to coerce value type".to_string())
        })
}

pub(crate) fn build_base_accumulator(
    kind: AggKind,
    base_udaf: &AggregateUDF,
    value_type: &DataType,
) -> Result<Box<dyn Accumulator>> {
    let coerced = if matches!(kind, AggKind::Count) {
        vec![value_type.clone()]
    } else {
        base_udaf.coerce_types(&[value_type.clone()])?
    };
    let return_type = base_udaf.return_type(&coerced)?;
    let input_field = Field::new("value", coerced[0].clone(), true);
    let schema = Schema::new(vec![input_field.clone()]);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];
    let return_field = Arc::new(Field::new("out", return_type, true));
    let args = AccumulatorArgs {
        return_field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: base_udaf.name(),
        is_distinct: false,
        exprs: &exprs,
    };
    if matches!(
        kind.accumulator_type(),
        AccumulatorType::RetractableAccumulator
    ) {
        base_udaf.create_sliding_accumulator(args)
    } else {
        base_udaf.accumulator(args)
    }
}

pub(crate) fn infer_value_type(kind: AggKind, state: &[Vec<u8>]) -> Result<DataType> {
    if state.is_empty() {
        return exec_err!("empty state");
    }
    let scalars: Vec<_> = state
        .iter()
        .map(|bytes| {
            scalar_value_from_bytes(bytes)
                .map_err(|error| datafusion::error::DataFusionError::Execution(error.to_string()))
        })
        .collect::<Result<_>>()?;
    let scalar = match kind {
        AggKind::Avg => scalars.get(1).or_else(|| scalars.first()),
        AggKind::Sum | AggKind::Min | AggKind::Max => scalars.first(),
        AggKind::Count => Some(&ScalarValue::Int64(Some(0))),
        _ => return exec_err!("unsupported aggregate kind for serialized state"),
    }
    .ok_or_else(|| {
        datafusion::error::DataFusionError::Execution("missing state value".to_string())
    })?;
    Ok(scalar.data_type())
}

pub fn merge_accumulator_state(
    accumulator: &mut dyn Accumulator,
    accumulator_state: &AccumulatorState,
) {
    let state_arrays: Vec<ArrayRef> = accumulator_state
        .iter()
        .map(|scalar| {
            scalar
                .to_array_of_size(1)
                .expect("Failed to convert scalar to array")
        })
        .collect();

    accumulator
        .merge_batch(&state_arrays)
        .expect("Failed to merge accumulator state");
}

/// Retract a previously merged accumulator state from a sliding accumulator.
pub fn retract_accumulator_state(
    window_expr: &Arc<dyn WindowExpr>,
    accumulator: &mut Box<dyn Accumulator>,
    tile_state: &AccumulatorState,
) {
    let mut current = accumulator
        .state()
        .expect("current accumulator state for tile retract");
    assert_eq!(
        current.len(),
        tile_state.len(),
        "tile state arity mismatch for retract"
    );
    for (current, tile) in current.iter_mut().zip(tile_state.iter()) {
        *current = scalar_saturating_sub(current, tile);
    }
    *accumulator = create_window_accumulator(window_expr);
    merge_accumulator_state(accumulator.as_mut(), &current);
}

pub(crate) fn apply_request_args(
    accumulator: &mut dyn Accumulator,
    request_args: Option<&[ArrayRef]>,
) {
    let Some(args) = request_args else {
        return;
    };
    accumulator.update_batch(args).expect("update request row");
}

fn scalar_saturating_sub(current: &ScalarValue, tile: &ScalarValue) -> ScalarValue {
    use ScalarValue::*;
    if tile.is_null() {
        return current.clone();
    }
    match (current, tile) {
        (Int64(Some(a)), Int64(Some(b))) => Int64(Some(a.saturating_sub(*b))),
        (Int32(Some(a)), Int32(Some(b))) => Int32(Some(a.saturating_sub(*b))),
        (Int16(Some(a)), Int16(Some(b))) => Int16(Some(a.saturating_sub(*b))),
        (Int8(Some(a)), Int8(Some(b))) => Int8(Some(a.saturating_sub(*b))),
        (UInt64(Some(a)), UInt64(Some(b))) => UInt64(Some(a.saturating_sub(*b))),
        (UInt32(Some(a)), UInt32(Some(b))) => UInt32(Some(a.saturating_sub(*b))),
        (Float64(Some(a)), Float64(Some(b))) => Float64(Some(a - b)),
        (Float32(Some(a)), Float32(Some(b))) => Float32(Some(a - b)),
        (a, b) => panic!(
            "unsupported scalar retract {:?} - {:?}",
            a.data_type(),
            b.data_type()
        ),
    }
}
