use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::tile::TileConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct WindowSpec {
    /// Retention padding after advance (`watermark − max_wl − lateness`).
    /// Default `0`: prune data below `W − max_wl`. Streaming late ingest is
    /// still gated by the task watermark (`ts ≤ frontier`), not this field.
    pub lateness: i64,
    /// Default tiling for all windows (overridable per-window via `tiling_configs`).
    pub tiling: Option<TileConfig>,
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            lateness: 0,
            tiling: None,
        }
    }
}

impl WindowSpec {
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
