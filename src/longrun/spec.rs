use std::path::PathBuf;
use std::time::Duration;

use crate::test_utils::checkpoint::WINDOW_RANGE_MS;
use crate::test_utils::harness::{PipelineLaunchSpec, RuntimeEnv};

use super::job::soak_window_launch_spec;

/// Soak window RANGE + event-time OOO (0). Used as the default lag p99 bound.
pub fn soak_lag_p99_bound() -> Duration {
    Duration::from_millis(WINDOW_RANGE_MS as u64)
}

/// Kind-calibrated defaults. Evaluated at soak teardown (Prom `query_range` + lifecycle).
#[derive(Clone, Debug)]
pub struct SoakOracleConfig {
    /// Fail if watermark lag is unchanged this long while `records_sent` still increases.
    pub watermark_stale_while_sending: Duration,
    /// Fail if lag p99 exceeds this for [`Self::lag_p99_sustain`].
    pub lag_p99: Duration,
    pub lag_p99_sustain: Duration,
    /// Fail if no completed checkpoint within this many `master.checkpoint_interval` multiples.
    pub checkpoint_silence_intervals: u32,
    /// Fail if checkpoint `failed` increases outside the injected-kill window.
    pub allow_checkpoint_fail_outside_kill: bool,
    /// Fail on WorkerPanic / HeartbeatUnavailable / pod restart outside KillAfterCheckpoint.
    pub allow_unexpected_fatal: bool,
    /// After restore, lag must be under bound within this window.
    pub restore_catchup: Duration,
}

impl Default for SoakOracleConfig {
    fn default() -> Self {
        Self {
            watermark_stale_while_sending: Duration::from_secs(30),
            lag_p99: soak_lag_p99_bound(),
            lag_p99_sustain: Duration::from_secs(60),
            checkpoint_silence_intervals: 3,
            allow_checkpoint_fail_outside_kill: false,
            allow_unexpected_fatal: false,
            restore_catchup: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoakScenario {
    Steady,
    KillAfterCheckpoint { after_checkpoint: u64 },
}

impl SoakScenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::KillAfterCheckpoint { .. } => "kill",
        }
    }
}

/// What [`PipelineLaunchSpec`] does not have: env, scenario, duration, later dump/oracles.
#[derive(Clone, Debug)]
pub struct SoakSpec {
    pub env: RuntimeEnv,
    pub launch: PipelineLaunchSpec,
    pub scenario: SoakScenario,
    pub duration: Duration,
    pub oracles: SoakOracleConfig,
    /// Teardown Prom dump directory (`query_range` JSON + oracle report).
    pub dump: Option<PathBuf>,
    /// Prometheus HTTP API (laptop port-forward). Required for Prom dump / Prom oracles.
    pub prom_url: Option<String>,
}

impl SoakSpec {
    pub fn new(env: RuntimeEnv, scenario: SoakScenario, duration: Duration) -> Self {
        Self {
            env,
            launch: soak_window_launch_spec(),
            scenario,
            duration,
            oracles: SoakOracleConfig::default(),
            dump: None,
            prom_url: None,
        }
    }

    pub fn default_duration_secs(env: RuntimeEnv) -> u64 {
        match env {
            RuntimeEnv::Local => 120,
            RuntimeEnv::Docker | RuntimeEnv::Kube => 3600,
        }
    }
}
