use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::transport::transport_spec::TransportSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRuntimeSpec {
    #[serde(default)]
    pub transport: TransportSpec,
    /// Snapshot history retention window, in milliseconds.
    /// When omitted, defaults to 10 minutes.
    #[serde(default)]
    pub history_retention_window_ms: Option<u64>,
}

impl Default for WorkerRuntimeSpec {
    fn default() -> Self {
        Self {
            transport: TransportSpec::default(),
            history_retention_window_ms: None,
        }
    }
}

impl WorkerRuntimeSpec {
    pub fn history_retention_window(&self) -> Duration {
        let ms = self
            .history_retention_window_ms
            .unwrap_or(10 * 60 * 1000)
            .max(1);
        Duration::from_millis(ms)
    }
}
