//! Checkpoint + fail + restore suite support (local + kube entrypoints live under `tests::inprocess` / `tests::kube`).

mod barrier;
mod kill_recovery;
mod launch;
mod mid_flight;
mod sink_oracle;
mod support;

pub use barrier::{assert_checkpoint_barrier_path, run_checkpoint_barrier_path};
pub use kill_recovery::{
    assert_checkpoint_multi_restore, assert_checkpoint_restore, run_checkpoint_sequential_failures,
    run_checkpoint_worker_kill_recovery, wait_for_kill_restore,
};
pub use launch::{
    checkpoint_multi_failure_launch_spec, checkpoint_recovery_launch_spec,
    checkpoint_shared_key_window_launch_spec, kube_checkpoint_spec, CheckpointWorkload,
    MULTI_FAILURE_COUNT, MULTI_WORKER_PARALLELISM, SINGLE_WORKER_PARALLELISM, WINDOW_RANGE_MS,
};
pub use mid_flight::{
    run_checkpoint_mid_flight_kill_after_safe, run_checkpoint_mid_flight_kill_no_prior,
};
pub use support::{
    wait_for_checkpoint_completed, wait_for_checkpoint_completed_id, wait_for_checkpoint_started,
    wait_until_attempt0_running, wait_until_attempt_running, wait_until_checkpoints_idle,
};
