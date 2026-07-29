use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::window_operator::WindowAdvancePolicy;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct WindowOperatorSpec {
    /// State retention after advance (`processed − max_wl − lateness`).
    /// Streaming late data is at or behind `processed_pos`.
    pub lateness: Option<i64>,
    pub advance_policy: WindowAdvancePolicy,
    /// Default tiling for all windows (overridable per-window via `tiling_configs`).
    pub tiling: Option<TileConfig>,
}

impl Default for WindowOperatorSpec {
    fn default() -> Self {
        Self {
            lateness: None,
            advance_policy: WindowAdvancePolicy::OnWatermark,
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
