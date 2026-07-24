use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::window_operator::RequestAdvancePolicy;
use crate::runtime::operators::window::window_operator_state::WindowId;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct WindowOperatorSpec {
    /// State retention after advance (`processed − max_wl − lateness`). Not ingest lag;
    /// streaming late is `ts ≤ processed_pos` (CDC late-data is separate).
    pub lateness: Option<i64>,
    pub request_advance_policy: RequestAdvancePolicy,
    /// Default tiling for all windows (overridable per-window via `tiling_configs`).
    pub tiling: Option<TileConfig>,
    /// Raw event `bucket_ts` width (ms) for SortedKV key layout / envelope scans.
    /// `None` → tiling min gran, else `60_000`. Not multi-row packing
    /// (see https://github.com/volga-project/volga/issues/155).
    pub raw_bucket_ms: Option<i64>,
}

impl Default for WindowOperatorSpec {
    fn default() -> Self {
        Self {
            lateness: None,
            request_advance_policy: RequestAdvancePolicy::OnWatermark,
            tiling: None,
            raw_bucket_ms: None,
        }
    }
}

impl WindowOperatorSpec {
    /// Pad/fill per-window tiling from overrides + `self.tiling` default.
    pub fn resolve_tiling(
        &self,
        n_windows: usize,
        tiling_overrides: &[Option<TileConfig>],
    ) -> Vec<Option<TileConfig>> {
        let mut out = tiling_overrides.to_vec();
        out.resize(n_windows, None);
        if let Some(default) = &self.tiling {
            for t in &mut out {
                if t.is_none() {
                    *t = Some(default.clone());
                }
            }
        }
        out
    }

    /// Resolve raw bucket width for store IO.
    pub fn resolve_bucket_ms(
        &self,
        window_configs: &BTreeMap<WindowId, WindowConfig>,
    ) -> i64 {
        if let Some(ms) = self.raw_bucket_ms {
            return ms.max(1);
        }
        for cfg in window_configs.values() {
            if let Some(t) = &cfg.tiling {
                return t.min_granularity().to_millis().max(1);
            }
        }
        60_000
    }
}
