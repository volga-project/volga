use arrow::array::ArrayRef;
use datafusion::common::Result;

use crate::runtime::utils::{scalar_value_from_bytes, scalar_value_to_bytes};

pub(crate) fn df_error(msg: impl Into<String>) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(msg.into())
}

pub(crate) use crate::runtime::operators::window::top::format::scalar_to_string;

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
