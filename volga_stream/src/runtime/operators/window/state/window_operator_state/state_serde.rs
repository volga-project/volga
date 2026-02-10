use super::WindowsState;
use crate::storage::write::state_serde::StateSerde;

fn encode_windows_state(value: &WindowsState) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn decode_windows_state(bytes: &[u8]) -> anyhow::Result<WindowsState> {
    Ok(bincode::deserialize(bytes)?)
}

fn estimate_windows_state(value: &WindowsState) -> usize {
    bincode::serialized_size(value).unwrap_or(0) as usize
}

pub fn windows_state_serde() -> StateSerde<WindowsState> {
    StateSerde::new(
        encode_windows_state,
        decode_windows_state,
        estimate_windows_state,
    )
}
