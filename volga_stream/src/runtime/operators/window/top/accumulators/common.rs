use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::AggKind;
use crate::runtime::utils::scalar_value_from_bytes;

pub(crate) fn df_error(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

pub(crate) fn scalar_from_array(array: &ArrayRef) -> Result<ScalarValue> {
    if array.len() == 0 {
        return exec_err!("empty top n array");
    }
    ScalarValue::try_from_array(array.as_ref(), 0).map_err(|e| df_error(e.to_string()))
}

pub(crate) fn scalar_to_usize(value: &ScalarValue) -> Result<usize> {
    match value {
        ScalarValue::Int32(Some(v)) => Ok(*v as usize),
        ScalarValue::Int64(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt32(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt64(Some(v)) => Ok(*v as usize),
        _ => exec_err!("top n must be integer"),
    }
}

pub(crate) fn parse_n_once(n: &mut Option<usize>, n_array: &ArrayRef) -> Result<usize> {
    if let Some(existing) = *n {
        return Ok(existing);
    }
    let value = scalar_from_array(n_array)?;
    let parsed = scalar_to_usize(&value)?;
    *n = Some(parsed);
    Ok(parsed)
}

pub(crate) fn parse_n_optional(
    n: &mut Option<usize>,
    n_array: Option<&ArrayRef>,
) -> Result<Option<usize>> {
    if n_array.is_none() {
        return Ok(None);
    }
    if let Some(existing) = *n {
        return Ok(Some(existing));
    }
    let value = scalar_from_array(n_array.expect("n array"))?;
    let parsed = scalar_to_usize(&value)?;
    *n = Some(parsed);
    Ok(Some(parsed))
}

pub(crate) fn infer_value_type(kind: AggKind, state: &[Vec<u8>]) -> Result<DataType> {
    if state.is_empty() {
        return exec_err!("empty state");
    }
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(state.len());
    for bytes in state {
        scalars.push(scalar_value_from_bytes(bytes).map_err(|e| df_error(e.to_string()))?);
    }
    let scalar = match kind {
        AggKind::Avg => scalars.get(1).or_else(|| scalars.get(0)),
        AggKind::Sum | AggKind::Min | AggKind::Max => scalars.get(0),
        AggKind::Count => Some(&ScalarValue::Int64(Some(0))),
        _ => return exec_err!("unsupported agg kind for top state"),
    }
    .ok_or_else(|| df_error("missing state value"))?;
    Ok(scalar.data_type())
}

pub(crate) fn ratio_scalar(matched: i64, total: i64) -> ScalarValue {
    if total <= 0 {
        ScalarValue::Null
    } else {
        ScalarValue::Float64(Some(matched as f64 / total as f64))
    }
}
