use std::time::Duration;

use crate::api::spec::connectors::{SinkSpec, SourceSpecKind};
use crate::api::PipelineSpec;
use crate::runtime::consts::RuntimeConstsProfile;
use crate::test_utils::checkpoint::{
    checkpoint_recovery_launch_spec, CheckpointWorkload, MULTI_WORKER_PARALLELISM,
};
use crate::test_utils::harness::PipelineLaunchSpec;

/// Sliding RANGE window job used by both Steady and KillAfterCheckpoint.
///
/// Same SQL as the checkpoint window suite; Count sink; Datagen `run_for_s` = soak duration;
/// prod runtime consts (30s checkpoint interval), not `kube_test` 2s.
pub fn soak_window_launch_spec(duration: Duration) -> PipelineLaunchSpec {
    let mut launch =
        checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM, CheckpointWorkload::Window);
    launch.pipeline.sink = Some(SinkSpec::Count);
    set_datagen_run_for_s(&mut launch.pipeline, duration.as_secs_f64());
    launch.expected_output_rows = 0;
    launch.with_runtime_consts_profile(RuntimeConstsProfile::Prod)
}

fn set_datagen_run_for_s(pipeline: &mut PipelineSpec, run_for_s: f64) {
    for source in &mut pipeline.sources {
        if let SourceSpecKind::Datagen(datagen) = &mut source.source {
            datagen.run_for_s = Some(run_for_s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_job_is_count_sink_window_prod() {
        let launch = soak_window_launch_spec(Duration::from_secs(120));
        assert!(matches!(launch.pipeline.sink, Some(SinkSpec::Count)));
        assert!(!launch
            .pipeline
            .sink
            .as_ref()
            .unwrap()
            .needs_in_memory_store());
        assert_eq!(launch.runtime_consts_profile, RuntimeConstsProfile::Prod);
        assert_eq!(launch.expected_output_rows, 0);
        assert_eq!(launch.worker_count, 2);
        let sql = launch.pipeline.sql.as_deref().expect("sql");
        assert!(sql.contains("RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING"));
        let run_for = launch
            .pipeline
            .sources
            .iter()
            .find_map(|source| match &source.source {
                SourceSpecKind::Datagen(datagen) => datagen.run_for_s,
                _ => None,
            });
        assert_eq!(run_for, Some(120.0));
    }
}
