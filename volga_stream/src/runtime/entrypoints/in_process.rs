use anyhow::Result;

use crate::api::PipelineSpec;
use crate::api::PipelineContext;
use crate::runtime::observability::PipelineSnapshot;

pub async fn run_in_process(spec: PipelineSpec) -> Result<PipelineSnapshot> {
    PipelineContext::new(spec)
        .execute_with_state_updates(None)
        .await
}
