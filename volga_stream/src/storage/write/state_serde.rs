use std::fmt::Debug;

pub type EncodeFn<V> = fn(&V) -> anyhow::Result<Vec<u8>>;
pub type DecodeFn<V> = fn(&[u8]) -> anyhow::Result<V>;
pub type EstimateFn<V> = fn(&V) -> usize;

pub struct StateSerde<V> {
    pub encode: EncodeFn<V>,
    pub decode: DecodeFn<V>,
    pub estimate: EstimateFn<V>,
}

impl<V> Copy for StateSerde<V> {}

impl<V> Clone for StateSerde<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> StateSerde<V> {
    pub fn new(encode: EncodeFn<V>, decode: DecodeFn<V>, estimate: EstimateFn<V>) -> Self {
        Self {
            encode,
            decode,
            estimate,
        }
    }
}

impl<V> Debug for StateSerde<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateSerde").finish()
    }
}

fn encode_bytes(value: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
    Ok(value.clone())
}

fn decode_bytes(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(bytes.to_vec())
}

fn estimate_bytes(value: &Vec<u8>) -> usize {
    value.len()
}

pub fn bytes_serde() -> StateSerde<Vec<u8>> {
    StateSerde::new(encode_bytes, decode_bytes, estimate_bytes)
}
