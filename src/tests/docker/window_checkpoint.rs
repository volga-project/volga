//! Local workers against the shared Scylla container on 127.0.0.1.

use anyhow::Result;

use crate::api::spec::state::{OperatorStateBackendConfig, ScyllaConfig};
use crate::test_utils::checkpoint::{
    assert_checkpoint_multi_restore, checkpoint_recovery_launch_spec,
    run_checkpoint_worker_kill_recovery, CheckpointWorkload, SINGLE_WORKER_PARALLELISM,
};
use crate::test_utils::harness::{RuntimeEnv, WorkerKillMode};

use super::scylla::{contact, unique_keyspace};

#[tokio::test]
#[ignore]
async fn test_docker_scylla_local_worker_window_checkpoint_restore() -> Result<()> {
    let launch =
        checkpoint_recovery_launch_spec(SINGLE_WORKER_PARALLELISM, CheckpointWorkload::Window)
            .with_operator_backend(OperatorStateBackendConfig::Scylla(ScyllaConfig {
                contact_points: vec![contact()],
                keyspace: unique_keyspace("volga_local_window"),
                datacenter: None,
            }));
    let report = run_checkpoint_worker_kill_recovery(
        RuntimeEnv::Local,
        launch,
        WorkerKillMode::Abrupt,
        CheckpointWorkload::Window,
    )
    .await?;
    assert_checkpoint_multi_restore(&report, 1, 1)
}
