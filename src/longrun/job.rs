use crate::api::spec::connectors::{SinkSpec, SourceSpecKind};
use crate::api::PipelineSpec;
use crate::runtime::consts::RuntimeConstsProfile;
use crate::test_utils::checkpoint::{
    checkpoint_recovery_launch_spec, CheckpointWorkload, MULTI_WORKER_PARALLELISM,
};
use crate::test_utils::harness::PipelineLaunchSpec;

/// Sliding RANGE window job used by both Steady and KillAfterCheckpoint.
///
/// Same SQL as the checkpoint window suite; Count sink; prod runtime consts
/// (30s checkpoint interval), not `kube_test` 2s.
///
/// Datagen `run_for_s` stays unset: the soak loop owns wall-clock duration and
/// shuts the cluster down. A per-attempt `run_for_s` would restart after kill
/// restore and overshoot the remaining soak window.
pub fn soak_window_launch_spec() -> PipelineLaunchSpec {
    let mut launch =
        checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM, CheckpointWorkload::Window);
    launch.pipeline.sink = Some(SinkSpec::Count);
    clear_datagen_run_for_s(&mut launch.pipeline);
    launch.expected_output_rows = None;
    launch.with_runtime_consts_profile(RuntimeConstsProfile::Prod)
}

fn clear_datagen_run_for_s(pipeline: &mut PipelineSpec) {
    for source in &mut pipeline.sources {
        if let SourceSpecKind::Datagen(datagen) = &mut source.source {
            datagen.run_for_s = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_job_is_count_sink_window_prod() {
        let launch = soak_window_launch_spec();
        assert!(matches!(launch.pipeline.sink, Some(SinkSpec::Count)));
        assert!(!launch
            .pipeline
            .sink
            .as_ref()
            .unwrap()
            .needs_in_memory_store());
        assert_eq!(launch.runtime_consts_profile, RuntimeConstsProfile::Prod);
        assert_eq!(launch.expected_output_rows, None);
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
        assert_eq!(run_for, None);
        assert_eq!(
            launch.pipeline.state.checkpoint,
            crate::api::CheckpointSpec::default()
        );
    }
}
