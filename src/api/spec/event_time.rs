use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Pipeline-wide event-time defaults (SQL / streaming).
///
/// Watermark knobs are shared; allowed lateness is namespaced by operator kind
/// (`window` now; `join` later) so semantics stay distinct.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventTimeSpec {
    #[serde(default)]
    pub watermark: WatermarkSpec,
    #[serde(default)]
    pub window: WindowEventTimeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WatermarkSpec {
    /// Max event-time reorder delay for auto-placed watermark assigners (ms).
    #[serde(default)]
    pub out_of_orderness_ms: u64,
    /// If set, idle upstreams older than this are excluded from min-WM merge.
    /// `None` keeps the runtime assigner default (`WatermarkAssignConfig::DEFAULT_IDLE_TIMEOUT_MS`).
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

impl Default for WatermarkSpec {
    fn default() -> Self {
        Self {
            out_of_orderness_ms: 0,
            idle_timeout_ms: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WindowEventTimeSpec {
    /// Allowed lateness for window operators (ms). `None` = no lateness.
    #[serde(default)]
    pub allowed_lateness_ms: Option<i64>,
}
