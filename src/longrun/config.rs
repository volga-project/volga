use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::test_utils::harness::RuntimeEnv;

use super::spec::{
    resolved_checkpoint_interval, soak_lag_p99_bound, SoakOracleConfig, SoakScenario, SoakSpec,
};

/// On-disk soak/bench run file. CLI flags override these fields.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoakFile {
    pub env: Option<String>,
    pub duration_secs: Option<u64>,
    pub scenario: Option<String>,
    pub kill_after_checkpoint: Option<u64>,
    pub dump: Option<PathBuf>,
    pub prom_url: Option<String>,
    pub launch: Option<SoakLaunchFile>,
    pub oracles: Option<SoakOracleFile>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoakLaunchFile {
    pub worker_count: Option<usize>,
    pub checkpoint: Option<SoakCheckpointFile>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoakCheckpointFile {
    pub interval_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub retention: Option<u64>,
}

/// Duration fields accept `30s` / `500ms` or multipliers (`1.5x_window`).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoakOracleFile {
    pub watermark_stale_while_sending: Option<String>,
    pub lag_p99: Option<String>,
    pub lag_p99_sustain: Option<String>,
    pub checkpoint_silence_intervals: Option<u32>,
    pub restore_catchup: Option<String>,
    pub allow_checkpoint_fail_outside_kill: Option<bool>,
    pub allow_unexpected_fatal: Option<bool>,
}

impl SoakFile {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read soak config {}", path.display()))?;
        serde_yaml::from_str(&raw).with_context(|| format!("parse soak config {}", path.display()))
    }

    pub fn env(&self) -> Result<Option<RuntimeEnv>> {
        self.env
            .as_deref()
            .map(|value| {
                RuntimeEnv::parse(value)
                    .ok_or_else(|| anyhow!("invalid config env `{value}` (local|kube|docker)"))
            })
            .transpose()
    }

    pub fn scenario(&self) -> Result<Option<SoakScenario>> {
        match self.scenario.as_deref() {
            None => Ok(None),
            Some("steady") => Ok(Some(SoakScenario::Steady)),
            Some("kill") => Ok(Some(SoakScenario::KillAfterCheckpoint {
                after_checkpoint: self.kill_after_checkpoint.unwrap_or(1),
            })),
            Some(other) => bail!("invalid config scenario `{other}` (steady|kill)"),
        }
    }
}

/// CLI values win over the file. Unset fields keep [`SoakSpec::new`] defaults.
pub fn apply_file(mut spec: SoakSpec, file: &SoakFile) -> Result<SoakSpec> {
    if let Some(env) = file.env()? {
        spec.env = env;
    }
    if let Some(secs) = file.duration_secs {
        if secs == 0 {
            bail!("config duration_secs must be >= 1");
        }
        spec.duration = Duration::from_secs(secs);
    }
    if let Some(scenario) = file.scenario()? {
        spec.scenario = scenario;
    }
    if let Some(dump) = &file.dump {
        spec.dump = Some(dump.clone());
    }
    if let Some(prom_url) = &file.prom_url {
        spec.prom_url = Some(prom_url.clone());
    }
    if let Some(launch) = &file.launch {
        if let Some(worker_count) = launch.worker_count {
            if worker_count == 0 {
                bail!("config launch.worker_count must be >= 1");
            }
            spec.launch.worker_count = worker_count;
        }
        if let Some(checkpoint) = &launch.checkpoint {
            apply_checkpoint(&mut spec.launch.pipeline.state.checkpoint, checkpoint)?;
        }
    }
    if let Some(oracles) = &file.oracles {
        let interval = resolved_checkpoint_interval(&spec.launch.pipeline.state.checkpoint);
        apply_oracles(&mut spec.oracles, oracles, interval)?;
    }
    Ok(spec)
}

fn apply_checkpoint(
    checkpoint: &mut crate::api::CheckpointSpec,
    file: &SoakCheckpointFile,
) -> Result<()> {
    if let Some(ms) = file.interval_ms {
        if ms == 0 {
            bail!("config launch.checkpoint.interval_ms must be > 0");
        }
        checkpoint.interval_ms = Some(ms);
    }
    if let Some(ms) = file.timeout_ms {
        if ms == 0 {
            bail!("config launch.checkpoint.timeout_ms must be > 0");
        }
        checkpoint.timeout_ms = Some(ms);
    }
    if let Some(n) = file.retention {
        if n == 0 {
            bail!("config launch.checkpoint.retention must be > 0");
        }
        checkpoint.retention = Some(n);
    }
    Ok(())
}

fn apply_oracles(
    oracles: &mut SoakOracleConfig,
    file: &SoakOracleFile,
    checkpoint_interval: Duration,
) -> Result<()> {
    if let Some(raw) = &file.watermark_stale_while_sending {
        oracles.watermark_stale_while_sending = parse_duration(raw, checkpoint_interval)?;
    }
    if let Some(raw) = &file.lag_p99 {
        oracles.lag_p99 = parse_duration(raw, checkpoint_interval)?;
    }
    if let Some(raw) = &file.lag_p99_sustain {
        oracles.lag_p99_sustain = parse_duration(raw, checkpoint_interval)?;
    }
    if let Some(n) = file.checkpoint_silence_intervals {
        if n == 0 {
            bail!("config oracles.checkpoint_silence_intervals must be >= 1");
        }
        oracles.checkpoint_silence_intervals = n;
    }
    if let Some(raw) = &file.restore_catchup {
        oracles.restore_catchup = parse_duration(raw, checkpoint_interval)?;
    }
    if let Some(v) = file.allow_checkpoint_fail_outside_kill {
        oracles.allow_checkpoint_fail_outside_kill = v;
    }
    if let Some(v) = file.allow_unexpected_fatal {
        oracles.allow_unexpected_fatal = v;
    }
    Ok(())
}

fn parse_duration(raw: &str, checkpoint_interval: Duration) -> Result<Duration> {
    let raw = raw.trim();
    if let Some(mult) = raw.strip_suffix("x_interval") {
        let n: f64 = mult
            .parse()
            .map_err(|_| anyhow!("invalid duration multiplier `{raw}`"))?;
        if n <= 0.0 {
            bail!("duration multiplier must be > 0: `{raw}`");
        }
        return Ok(Duration::from_secs_f64(
            checkpoint_interval.as_secs_f64() * n,
        ));
    }
    if let Some(mult) = raw.strip_suffix("x_window") {
        let n: f64 = mult
            .parse()
            .map_err(|_| anyhow!("invalid duration multiplier `{raw}`"))?;
        if n <= 0.0 {
            bail!("duration multiplier must be > 0: `{raw}`");
        }
        return Ok(Duration::from_secs_f64(
            soak_lag_p99_bound().as_secs_f64() * n,
        ));
    }
    if let Some(rest) = raw.strip_suffix("ms") {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid duration `{raw}`"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(rest) = raw.strip_suffix('s') {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid duration `{raw}`"))?;
        return Ok(Duration::from_secs(n));
    }
    bail!("invalid duration `{raw}` (use Ns, Nms, Nx_window, or Nx_interval)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_window_multiplier() {
        let interval = Duration::from_secs(30);
        assert_eq!(parse_duration("1.5x_window", interval).unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("3x_interval", interval).unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30s", interval).unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms", interval).unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn apply_file_sets_oracles_and_launch() {
        let file: SoakFile = serde_yaml::from_str(
            r#"
env: kube
duration_secs: 3600
scenario: kill
kill_after_checkpoint: 2
dump: /tmp/soak
prom_url: http://127.0.0.1:9090
launch:
  worker_count: 4
  checkpoint:
    interval_ms: 5000
    timeout_ms: 15000
    retention: 2
oracles:
  lag_p99: 1.5x_window
  restore_catchup: 3x_interval
  checkpoint_silence_intervals: 4
"#,
        )
        .unwrap();
        let spec = apply_file(
            SoakSpec::new(
                RuntimeEnv::Local,
                SoakScenario::Steady,
                Duration::from_secs(120),
            ),
            &file,
        )
        .unwrap();
        assert_eq!(spec.env, RuntimeEnv::Kube);
        assert_eq!(spec.duration, Duration::from_secs(3600));
        assert_eq!(
            spec.scenario,
            SoakScenario::KillAfterCheckpoint {
                after_checkpoint: 2
            }
        );
        assert_eq!(spec.launch.worker_count, 4);
        assert_eq!(spec.launch.pipeline.state.checkpoint.interval_ms, Some(5_000));
        assert_eq!(spec.launch.pipeline.state.checkpoint.retention, Some(2));
        assert_eq!(spec.oracles.lag_p99, Duration::from_secs(15));
        assert_eq!(spec.oracles.restore_catchup, Duration::from_secs(15));
        assert_eq!(spec.oracles.checkpoint_silence_intervals, 4);
    }
}
