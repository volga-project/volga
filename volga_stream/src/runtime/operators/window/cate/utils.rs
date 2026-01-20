use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::AggregateUDF;
use datafusion::scalar::ScalarValue;

use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

use super::types::AggKind;

pub(crate) fn df_error(msg: impl Into<String>) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(msg.into())
}

pub(crate) fn format_float(value: f64) -> String {
    let s = format!("{value:.6}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

pub(crate) fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(format_float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(format_float(*v)),
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        ScalarValue::Decimal128(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        ScalarValue::Decimal256(Some(v), p, s) => Some(format!("{v}({p},{s})")),
        _ => None,
    }
}

pub(crate) fn acc_state_to_bytes(
    acc: &mut dyn datafusion::logical_expr::Accumulator,
) -> Result<Vec<Vec<u8>>> {
    let vals = acc.state()?;
    vals.into_iter()
        .map(|v| {
            scalar_value_to_bytes(&v).map_err(|e| df_error(format!("state encode failed: {e}")))
        })
        .collect()
}

pub(crate) fn merge_state_bytes(
    acc: &mut dyn datafusion::logical_expr::Accumulator,
    state: &[Vec<u8>],
) -> Result<()> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(state.len());
    for bytes in state {
        let v = scalar_value_from_bytes(bytes)
            .map_err(|e| df_error(format!("state decode failed: {e}")))?;
        let arr = v
            .to_array_of_size(1)
            .map_err(|e| df_error(format!("state array convert failed: {e}")))?;
        arrays.push(arr);
    }
    acc.merge_batch(&arrays)
}

pub(crate) fn infer_value_type(kind: AggKind, state: &[Vec<u8>]) -> Result<DataType> {
    if state.is_empty() {
        return exec_err!("empty state");
    }
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(state.len());
    for bytes in state {
        scalars.push(
            scalar_value_from_bytes(bytes)
                .map_err(|e| df_error(format!("state decode failed: {e}")))?,
        );
    }
    let scalar = match kind {
        AggKind::Avg => scalars.get(1).or_else(|| scalars.get(0)),
        AggKind::Sum | AggKind::Min | AggKind::Max => scalars.get(0),
        AggKind::Count => Some(&ScalarValue::Int64(Some(0))),
        _ => return exec_err!("unsupported agg kind for cate state"),
    }
    .ok_or_else(|| df_error("missing state value"))?;
    Ok(scalar.data_type())
}

pub(crate) fn coerce_value_type(
    kind: AggKind,
    base_udaf: &AggregateUDF,
    value_type: &DataType,
) -> Result<DataType> {
    if matches!(kind, AggKind::Count) {
        return Ok(value_type.clone());
    }
    let coerced = base_udaf.coerce_types(&[value_type.clone()])?;
    if coerced.is_empty() {
        return exec_err!("failed to coerce value type");
    }
    Ok(coerced[0].clone())
}
