use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Pipeline-wide event-time defaults (SQL / streaming).
///
/// Watermark knobs are shared; window `allowed_lateness_ms` is retention-only
/// (prune after advance). Streaming late ingest is `ts ≤ processed_pos`.
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
    /// Window state retention after advance (ms). `None` = no prune.
    /// Does not delay watermarks or accept post-frontier ingest; CDC late-data is separate.
    #[serde(default)]
    pub allowed_lateness_ms: Option<i64>,
}
