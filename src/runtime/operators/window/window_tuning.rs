use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::window_operator::RequestAdvancePolicy;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct WindowOperatorSpec {
    /// State retention after advance (`processed − max_wl − lateness`). Not ingest lag;
    /// streaming late is `ts ≤ processed_pos` (CDC late-data is separate).
    pub lateness: Option<i64>,
    pub request_advance_policy: RequestAdvancePolicy,
    /// Default tiling for all windows (overridable per-window via `tiling_configs`).
    pub tiling: Option<TileConfig>,
}

impl Default for WindowOperatorSpec {
    fn default() -> Self {
        Self {
            lateness: None,
            request_advance_policy: RequestAdvancePolicy::OnWatermark,
            tiling: None,
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
}
