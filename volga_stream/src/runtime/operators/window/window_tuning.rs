use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::state::tiles::TileConfig;
use crate::runtime::operators::window::window_operator::RequestAdvancePolicy;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowOperatorSpec {
    pub lateness: Option<i64>,
    pub request_advance_policy: RequestAdvancePolicy,
    pub compaction_interval_ms: u64,
    pub dump_interval_ms: u64,
    pub dump_hot_bucket_count: usize,
    pub in_mem_dump_parallelism: usize,
    pub tiling: Option<TileConfig>,
    pub parallelize: bool,
}

impl Default for WindowOperatorSpec {
    fn default() -> Self {
        Self {
            lateness: None,
            request_advance_policy: RequestAdvancePolicy::OnWatermark,
            compaction_interval_ms: 250,
            dump_interval_ms: 1000,
            dump_hot_bucket_count: 2,
            in_mem_dump_parallelism: 4,
            tiling: None,
            parallelize: false,
        }
    }
}

