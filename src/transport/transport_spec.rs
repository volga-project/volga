use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorTransportSpec {
    pub queue_records: Option<u32>,
}

impl Default for OperatorTransportSpec {
    fn default() -> Self {
        Self { queue_records: None }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransportSpec {
    pub default_queue_records: u32,
}

impl TransportSpec {
    pub const DEFAULT_QUEUE_RECORDS: u32 = 8192;
}

impl Default for TransportSpec {
    fn default() -> Self {
        Self {
            default_queue_records: Self::DEFAULT_QUEUE_RECORDS,
        }
    }
}

