use crate::common::Key;

/// Logical namespace shared by WO and WRO.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateNamespace {
    pub bytes: Vec<u8>,
}

impl StateNamespace {
    pub fn new(s: impl AsRef<[u8]>) -> Self {
        Self {
            bytes: s.as_ref().to_vec(),
        }
    }
}

/// Collision-safe logical identity. Backends choose their own physical keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub namespace: Vec<u8>,
    pub business_key: Vec<u8>,
}

impl PartitionKey {
    pub fn new(namespace: &StateNamespace, key: &Key) -> Self {
        Self {
            namespace: namespace.bytes.clone(),
            business_key: key.to_bytes(),
        }
    }
}
