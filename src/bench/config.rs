use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use serde::Deserialize;

use crate::api::spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::operators::{OperatorOverride, OperatorTuningSpec};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{DatagenSpec, PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::consts::RuntimeConstsProfile;
use crate::runtime::functions::source::datagen_source::FieldGenerator;
use crate::runtime::operators::window::model::TimeGranularity;
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::tile::TileConfig;
use crate::test_utils::harness::{PipelineLaunchSpec, RuntimeEnv};

use super::dump::extra_prom_queries;
use super::spec::{OracleConfig, Scenario, BenchSpec};

const DEFAULT_PARALLELISM: usize = 4;
const DEFAULT_SLOTS_PER_NODE: usize = 2;
const DEFAULT_THREADS_PER_TASK: usize = 2;
const DEFAULT_DATAGEN_RATE: f32 = 200.0;
const DEFAULT_DATAGEN_BATCH: usize = 20;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CHECKPOINT_RETENTION: u64 = 1;
const DEFAULT_SQL: &str = "SELECT timestamp, key, value, \
     SUM(value) OVER w AS sum_val, \
     COUNT(value) OVER w AS cnt_val, \
     AVG(value) OVER w AS avg_val, \
     MIN(value) OVER w AS min_val, \
     MAX(value) OVER w AS max_val \
     FROM datagen_source \
     WINDOW w AS (PARTITION BY key ORDER BY timestamp \
     RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW)";

/// On-disk bench spec. CLI `--env` / `--duration-secs` override these two fields.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchFile {
    pub env: Option<String>,
    pub duration: Option<String>,
    pub scenario: Option<String>,
    pub kill_after_checkpoint: Option<u64>,
    pub dump: Option<PathBuf>,
    pub prom_url: Option<String>,
    pub launch: Option<LaunchFile>,
    pub oracles: Option<OracleFile>,
    /// Dump-only PromQL (`name` + `query`). Required oracle queries stay in code.
    #[serde(default)]
    pub extra_prom_queries: Option<Vec<NamedPromQuery>>,
}

/// Job overlay compiled into `PipelineSpec`. Count sink + Datagen source are fixed in code.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchFile {
    pub sql: Option<String>,
    pub parallelism: Option<usize>,
    pub slots_per_node: Option<usize>,
    pub threads_per_task: Option<usize>,
    pub checkpoint: Option<CheckpointFile>,
    pub datagen: Option<DatagenFile>,
    pub window: Option<WindowFile>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointFile {
    pub interval: Option<String>,
    pub timeout: Option<String>,
    pub retention: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatagenFile {
    pub rate: Option<f32>,
    pub batch_size: Option<usize>,
    pub num_unique: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowFile {
    pub tiling: Option<Vec<String>>,
    pub out_of_orderness_ms: Option<u64>,
    pub allowed_lateness_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NamedPromQuery {
    pub name: String,
    pub query: String,
}

/// Duration fields accept `1h` / `30s` / `500ms`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleFile {
    pub watermark_stale_while_sending: Option<String>,
    pub lag_p99: Option<String>,
    pub lag_p99_sustain: Option<String>,
    pub checkpoint_silence_intervals: Option<u32>,
    pub restore_catchup: Option<String>,
    pub allow_checkpoint_fail_outside_kill: Option<bool>,
    pub allow_unexpected_fatal: Option<bool>,
}

impl BenchFile {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read bench config {}", path.display()))?;
        serde_yaml::from_str(&raw).with_context(|| format!("parse bench config {}", path.display()))
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

    pub fn scenario(&self) -> Result<Option<Scenario>> {
        if let Some(n) = self.kill_after_checkpoint {
            if n == 0 {
                bail!("config kill_after_checkpoint must be >= 1");
            }
            if self.scenario.as_deref() != Some("kill") {
                bail!("config kill_after_checkpoint requires scenario: kill");
            }
        }
        match self.scenario.as_deref() {
            None => Ok(None),
            Some("steady") => Ok(Some(Scenario::Steady)),
            Some("kill") => Ok(Some(Scenario::KillAfterCheckpoint {
                after_checkpoint: self.kill_after_checkpoint.unwrap_or(1),
            })),
            Some(other) => bail!("invalid config scenario `{other}` (steady|kill)"),
        }
    }
}

/// Build a bench spec from the run file. Omitted `launch` uses the default window job.
pub fn apply_file(file: &BenchFile) -> Result<BenchSpec> {
    let env = file.env()?.unwrap_or(RuntimeEnv::Local);
    let duration = match &file.duration {
        Some(raw) => parse_duration(raw)?,
        None => Duration::from_secs(BenchSpec::default_duration_secs(env)),
    };
    let mut oracles = OracleConfig::default();
    if let Some(file_oracles) = &file.oracles {
        apply_oracles(&mut oracles, file_oracles)?;
    }
    let extra = match &file.extra_prom_queries {
        Some(queries) => queries
            .iter()
            .map(|q| (q.name.clone(), q.query.clone()))
            .collect(),
        None => extra_prom_queries(),
    };
    Ok(BenchSpec {
        env,
        launch: compile_launch(file.launch.as_ref())?,
        scenario: file.scenario()?.unwrap_or(Scenario::Steady),
        duration,
        oracles,
        dump: file.dump.clone(),
        prom_url: file.prom_url.clone(),
        extra_prom_queries: extra,
    })
}

fn compile_launch(launch: Option<&LaunchFile>) -> Result<PipelineLaunchSpec> {
    let launch = launch.cloned().unwrap_or_default();
    let parallelism = nonzero_usize(launch.parallelism, DEFAULT_PARALLELISM, "launch.parallelism")?;
    let slots_per_node =
        nonzero_usize(launch.slots_per_node, DEFAULT_SLOTS_PER_NODE, "launch.slots_per_node")?;
    if parallelism % slots_per_node != 0 {
        bail!(
            "launch.parallelism {parallelism} is not divisible by slots_per_node {slots_per_node}"
        );
    }
    let worker_count = parallelism / slots_per_node;
    let threads_per_task = nonzero_usize(
        launch.threads_per_task,
        DEFAULT_THREADS_PER_TASK,
        "launch.threads_per_task",
    )?;
    let sql = launch
        .sql
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SQL);
    let (interval_ms, timeout_ms, retention) = checkpoint_from_file(launch.checkpoint.as_ref())?;
    let datagen = datagen_from_file(launch.datagen.as_ref(), parallelism)?;
    let (tiling, out_of_orderness_ms, allowed_lateness_ms) =
        window_from_file(launch.window.as_ref())?;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: threads_per_task,
        })
        .with_task_assignment_strategy(TaskWorkerAssignmentStrategyType::Pipelined {
            slots_per_node,
        })
        .with_source(SourceSpec::new(
            "datagen_source",
            SourceSpecKind::Datagen(datagen),
            schema_to_json(schema.as_ref()),
        ))
        .with_out_of_orderness_ms(out_of_orderness_ms)
        .with_window_allowed_lateness_ms(allowed_lateness_ms)
        .with_operator_overrides_defaults(OperatorOverride {
            tuning: Some(OperatorTuningSpec::Window(WindowSpec {
                lateness: allowed_lateness_ms,
                tiling: Some(tiling),
            })),
            ..OperatorOverride::default()
        })
        .sql(sql)
        .with_sink(SinkSpec::Count)
        .with_checkpoint(Some(interval_ms), Some(timeout_ms), Some(retention))
        .build();

    Ok(
        PipelineLaunchSpec::new(pipeline, worker_count, None)
            .with_runtime_consts_profile(RuntimeConstsProfile::Prod),
    )
}

fn checkpoint_from_file(file: Option<&CheckpointFile>) -> Result<(u64, u64, u64)> {
    let file = file.cloned().unwrap_or_default();
    let interval_ms = duration_ms(
        file.interval.as_deref(),
        DEFAULT_CHECKPOINT_INTERVAL,
        "launch.checkpoint.interval",
    )?;
    if interval_ms == 0 {
        bail!("launch.checkpoint.interval must be > 0");
    }
    let timeout_ms = duration_ms(
        file.timeout.as_deref(),
        DEFAULT_CHECKPOINT_TIMEOUT,
        "launch.checkpoint.timeout",
    )?;
    if timeout_ms == 0 {
        bail!("launch.checkpoint.timeout must be > 0");
    }
    let retention = file.retention.unwrap_or(DEFAULT_CHECKPOINT_RETENTION);
    if retention == 0 {
        bail!("launch.checkpoint.retention must be >= 1");
    }
    Ok((interval_ms, timeout_ms, retention))
}

fn datagen_from_file(file: Option<&DatagenFile>, parallelism: usize) -> Result<DatagenSpec> {
    let file = file.cloned().unwrap_or_default();
    let rate = file.rate.unwrap_or(DEFAULT_DATAGEN_RATE);
    if rate <= 0.0 {
        bail!("launch.datagen.rate must be > 0");
    }
    let batch_size = nonzero_usize(
        file.batch_size,
        DEFAULT_DATAGEN_BATCH,
        "launch.datagen.batch_size",
    )?;
    let num_unique = nonzero_usize(
        file.num_unique,
        parallelism * 4,
        "launch.datagen.num_unique",
    )?;
    let mut fields = HashMap::new();
    // Wall-clock event time so watermark lag is `now − watermark`, not ~55 years
    // (IncrementalTimestamp starts at 1s and lag uses UNIX epoch ms).
    fields.insert(
        "timestamp".to_string(),
        FieldGenerator::ProcessingTimestamp,
    );
    fields.insert(
        "key".to_string(),
        FieldGenerator::Key { num_unique },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(2.0)),
            ],
        },
    );
    Ok(DatagenSpec {
        rate: Some(rate),
        limit: None,
        run_for_s: None,
        batch_size,
        fields,
        replayable: true,
    })
}

fn window_from_file(file: Option<&WindowFile>) -> Result<(TileConfig, u64, i64)> {
    let file = file.cloned().unwrap_or_default();
    let tiling_raw = file
        .tiling
        .unwrap_or_else(|| vec!["1s".to_string(), "5s".to_string()]);
    if tiling_raw.is_empty() {
        bail!("launch.window.tiling must not be empty");
    }
    let mut granularities = Vec::new();
    for raw in &tiling_raw {
        granularities.push(parse_granularity(raw)?);
    }
    let tiling = TileConfig::new(granularities).map_err(|error| anyhow!("{error}"))?;
    Ok((
        tiling,
        file.out_of_orderness_ms.unwrap_or(0),
        file.allowed_lateness_ms.unwrap_or(0),
    ))
}

fn nonzero_usize(value: Option<usize>, default: usize, field: &str) -> Result<usize> {
    match value {
        None => Ok(default),
        Some(0) => bail!("{field} must be >= 1"),
        Some(n) => Ok(n),
    }
}

fn duration_ms(raw: Option<&str>, default: Duration, field: &str) -> Result<u64> {
    let duration = match raw {
        None => default,
        Some(raw) => parse_duration(raw)?,
    };
    let ms = duration.as_millis();
    if ms == 0 || ms > u64::MAX as u128 {
        bail!("{field} must be > 0 and fit in milliseconds");
    }
    Ok(ms as u64)
}

fn parse_granularity(raw: &str) -> Result<TimeGranularity> {
    let raw = raw.trim();
    if let Some(rest) = raw.strip_suffix("ms") {
        let n: u32 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid window tiling `{raw}`"))?;
        if n == 0 || n % 1000 != 0 {
            bail!("window tiling `{raw}` must be a whole number of seconds");
        }
        return Ok(TimeGranularity::Seconds(n / 1000));
    }
    if let Some(rest) = raw.strip_suffix('h') {
        let n: u32 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid window tiling `{raw}`"))?;
        if n == 0 {
            bail!("window tiling must be > 0: `{raw}`");
        }
        return Ok(TimeGranularity::Hours(n));
    }
    if let Some(rest) = raw.strip_suffix('m') {
        let n: u32 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid window tiling `{raw}`"))?;
        if n == 0 {
            bail!("window tiling must be > 0: `{raw}`");
        }
        return Ok(TimeGranularity::Minutes(n));
    }
    if let Some(rest) = raw.strip_suffix('s') {
        let n: u32 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid window tiling `{raw}`"))?;
        if n == 0 {
            bail!("window tiling must be > 0: `{raw}`");
        }
        return Ok(TimeGranularity::Seconds(n));
    }
    bail!("invalid window tiling `{raw}` (use Ns, Nm, or Nh)")
}

fn apply_oracles(oracles: &mut OracleConfig, file: &OracleFile) -> Result<()> {
    if let Some(raw) = &file.watermark_stale_while_sending {
        oracles.watermark_stale_while_sending = parse_duration(raw)?;
    }
    if let Some(raw) = &file.lag_p99 {
        oracles.lag_p99 = parse_duration(raw)?;
    }
    if let Some(raw) = &file.lag_p99_sustain {
        oracles.lag_p99_sustain = parse_duration(raw)?;
    }
    if let Some(n) = file.checkpoint_silence_intervals {
        if n == 0 {
            bail!("config oracles.checkpoint_silence_intervals must be >= 1");
        }
        oracles.checkpoint_silence_intervals = n;
    }
    if let Some(raw) = &file.restore_catchup {
        oracles.restore_catchup = parse_duration(raw)?;
    }
    if let Some(v) = file.allow_checkpoint_fail_outside_kill {
        oracles.allow_checkpoint_fail_outside_kill = v;
    }
    if let Some(v) = file.allow_unexpected_fatal {
        oracles.allow_unexpected_fatal = v;
    }
    Ok(())
}

fn parse_duration(raw: &str) -> Result<Duration> {
    let raw = raw.trim();
    if let Some(rest) = raw.strip_suffix("ms") {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid duration `{raw}`"))?;
        if n == 0 {
            bail!("duration must be > 0: `{raw}`");
        }
        return Ok(Duration::from_millis(n));
    }
    if let Some(rest) = raw.strip_suffix('h') {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid duration `{raw}`"))?;
        if n == 0 {
            bail!("duration must be > 0: `{raw}`");
        }
        return Ok(Duration::from_secs(n.saturating_mul(3600)));
    }
    if let Some(rest) = raw.strip_suffix('s') {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid duration `{raw}`"))?;
        if n == 0 {
            bail!("duration must be > 0: `{raw}`");
        }
        return Ok(Duration::from_secs(n));
    }
    bail!("invalid duration `{raw}` (use Nh, Ns, or Nms)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::spec::connectors::SinkSpec;

    #[test]
    fn apply_file_sets_oracles_and_launch() {
        let file: BenchFile = serde_yaml::from_str(
            r#"
env: kube
duration: 1h
scenario: kill
kill_after_checkpoint: 2
dump: /tmp/soak
prom_url: http://127.0.0.1:9090
launch:
  parallelism: 8
  slots_per_node: 2
  checkpoint:
    interval: 30s
    timeout: 60s
    retention: 1
  datagen:
    rate: 100
    num_unique: 32
  sql: SELECT timestamp, key, value FROM datagen_source
oracles:
  lag_p99: 15s
  restore_catchup: 90s
  checkpoint_silence_intervals: 4
extra_prom_queries:
  - name: custom
    query: up
"#,
        )
        .unwrap();
        let spec = apply_file(&file).unwrap();
        assert_eq!(spec.env, RuntimeEnv::Kube);
        assert_eq!(spec.duration, Duration::from_secs(3600));
        assert_eq!(
            spec.scenario,
            Scenario::KillAfterCheckpoint {
                after_checkpoint: 2
            }
        );
        assert_eq!(spec.launch.worker_count, 4);
        assert_eq!(spec.launch.pipeline.parallelism, 8);
        assert_eq!(spec.launch.pipeline.state.checkpoint.interval_ms, Some(30_000));
        assert!(matches!(spec.launch.pipeline.sink, Some(SinkSpec::Count)));
        assert_eq!(
            spec.launch.pipeline.sql.as_deref(),
            Some("SELECT timestamp, key, value FROM datagen_source")
        );
        assert_eq!(spec.oracles.lag_p99, Duration::from_secs(15));
        assert_eq!(spec.oracles.restore_catchup, Duration::from_secs(90));
        assert_eq!(spec.oracles.checkpoint_silence_intervals, 4);
        assert_eq!(
            spec.extra_prom_queries,
            vec![("custom".to_string(), "up".to_string())]
        );
    }
}
