//! Checkpoint + fail + restore e2e (local + kube), same shape as recovery_tests.

pub mod kube;
pub mod local;

mod barrier;
mod kill_recovery;
mod launch;
mod mid_flight;
mod sink_oracle;
mod support;

pub use barrier::{assert_checkpoint_barrier_path, run_checkpoint_barrier_path};
pub use kill_recovery::{
    assert_checkpoint_multi_restore, assert_checkpoint_restore, run_checkpoint_sequential_failures,
    run_checkpoint_worker_kill_recovery,
};
pub use launch::{
    checkpoint_multi_failure_launch_spec, checkpoint_recovery_launch_spec, MULTI_FAILURE_COUNT,
    MULTI_WORKER_PARALLELISM, SINGLE_WORKER_PARALLELISM,
};
pub use mid_flight::{
    assert_mid_flight_restore_none, assert_mid_flight_restore_prior,
    run_checkpoint_mid_flight_kill_after_safe, run_checkpoint_mid_flight_kill_no_prior,
};
