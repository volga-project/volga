use std::time::Duration;

use anyhow::{anyhow, bail, Result};

use crate::test_utils::harness::RuntimeEnv;

use super::config::{apply_file, SoakFile};
use super::run::run_soak;
use super::spec::{SoakScenario, SoakSpec};

const USAGE: &str = "\
Usage:
  volga-longrun soak [--config FILE] [--env local|kube|docker] [--scenario steady|kill]
                     [--duration-secs N] [--kill-after-checkpoint N] [--dump DIR] [--prom-url URL]
  volga-longrun bench

v1 implements soak only. Default duration: local 120s, kube/docker 3600s.
--config YAML sets env/duration/scenario/dump/oracles and a job overlay (sql, checkpoint, parallelism, datagen, window).
Count sink and Datagen source are fixed. CLI flags override run knobs.
--dump writes lifecycle.json (and prom.json when --prom-url is set).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongrunCommand {
    Soak(SoakArgs),
    Bench,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoakArgs {
    pub config: Option<std::path::PathBuf>,
    pub env: Option<RuntimeEnv>,
    pub scenario: Option<SoakScenario>,
    pub duration_secs: Option<u64>,
    pub dump: Option<std::path::PathBuf>,
    pub prom_url: Option<String>,
    pub kill_after: Option<u64>,
}

pub async fn run<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    match parse_args(args)? {
        LongrunCommand::Soak(args) => run_soak(build_spec(args)?).await,
        LongrunCommand::Bench => {
            bail!("`volga-longrun bench` is not implemented in v1; use `soak`")
        }
    }
}

pub fn parse_args<I, S>(args: I) -> Result<LongrunCommand>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        bail!("{USAGE}");
    }
    match args[0].as_str() {
        "soak" => Ok(LongrunCommand::Soak(parse_soak(&args[1..])?)),
        "bench" => {
            if args.len() > 1 {
                bail!("bench takes no arguments\n{USAGE}");
            }
            Ok(LongrunCommand::Bench)
        }
        other => bail!("unknown command `{other}`\n{USAGE}"),
    }
}

fn parse_soak(args: &[String]) -> Result<SoakArgs> {
    let mut config = None;
    let mut env = None;
    let mut scenario_kill = None;
    let mut duration_secs = None;
    let mut kill_after = None;
    let mut dump = None;
    let mut prom_url = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let (flag, inline) = split_flag(arg);
        match flag {
            "--config" => {
                config = Some(std::path::PathBuf::from(take_value(flag, inline, args, &mut i)?));
            }
            "--env" => {
                let value = take_value(flag, inline, args, &mut i)?;
                env = Some(RuntimeEnv::parse(&value).ok_or_else(|| {
                    anyhow!("invalid --env `{value}` (local|kube|docker)")
                })?);
            }
            "--scenario" => {
                let value = take_value(flag, inline, args, &mut i)?;
                scenario_kill = Some(match value.as_str() {
                    "steady" => false,
                    "kill" => true,
                    other => bail!("invalid --scenario `{other}` (steady|kill)"),
                });
            }
            "--duration-secs" => {
                let value = take_value(flag, inline, args, &mut i)?;
                let n: u64 = value
                    .parse()
                    .map_err(|_| anyhow!("invalid --duration-secs `{value}`"))?;
                if n == 0 {
                    bail!("--duration-secs must be >= 1");
                }
                duration_secs = Some(n);
            }
            "--kill-after-checkpoint" => {
                let value = take_value(flag, inline, args, &mut i)?;
                let n: u64 = value
                    .parse()
                    .map_err(|_| anyhow!("invalid --kill-after-checkpoint `{value}`"))?;
                if n == 0 {
                    bail!("--kill-after-checkpoint must be >= 1");
                }
                kill_after = Some(n);
            }
            "--dump" => {
                dump = Some(std::path::PathBuf::from(take_value(flag, inline, args, &mut i)?));
            }
            "--prom-url" => {
                prom_url = Some(take_value(flag, inline, args, &mut i)?);
            }
            other => bail!("unknown soak flag `{other}`\n{USAGE}"),
        }
        i += 1;
    }
    if kill_after.is_some() && scenario_kill == Some(false) {
        bail!("--kill-after-checkpoint requires --scenario kill");
    }
    if kill_after.is_some() && scenario_kill.is_none() && config.is_none() {
        bail!("--kill-after-checkpoint requires --scenario kill");
    }
    let scenario = match scenario_kill {
        Some(false) => Some(SoakScenario::Steady),
        Some(true) => Some(SoakScenario::KillAfterCheckpoint {
            after_checkpoint: kill_after.unwrap_or(1),
        }),
        None => None,
    };
    Ok(SoakArgs {
        config,
        env,
        scenario,
        duration_secs,
        dump,
        prom_url,
        kill_after,
    })
}

fn build_spec(args: SoakArgs) -> Result<SoakSpec> {
    let file = match &args.config {
        Some(path) => SoakFile::load(path)?,
        None => SoakFile::default(),
    };
    let mut spec = apply_file(&file)?;
    if let Some(env) = args.env {
        spec.env = env;
    }
    if let Some(secs) = args.duration_secs {
        spec.duration = Duration::from_secs(secs);
    } else if file.duration.is_none() {
        spec.duration = Duration::from_secs(SoakSpec::default_duration_secs(spec.env));
    }
    if let Some(scenario) = args.scenario {
        spec.scenario = scenario;
    }
    if let Some(n) = args.kill_after {
        match &mut spec.scenario {
            SoakScenario::KillAfterCheckpoint { after_checkpoint } => {
                *after_checkpoint = n;
            }
            SoakScenario::Steady => {
                bail!("--kill-after-checkpoint requires --scenario kill or config scenario: kill")
            }
        }
    }
    if let Some(dump) = args.dump {
        spec.dump = Some(dump);
    }
    if let Some(prom_url) = args.prom_url {
        spec.prom_url = Some(prom_url);
    }
    Ok(spec)
}

fn split_flag(arg: &str) -> (&str, Option<&str>) {
    match arg.split_once('=') {
        Some((flag, value)) => (flag, Some(value)),
        None => (arg, None),
    }
}

fn take_value(flag: &str, inline: Option<&str>, args: &[String], i: &mut usize) -> Result<String> {
    if let Some(value) = inline {
        return Ok(value.to_string());
    }
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_soak_kill_dump() {
        let LongrunCommand::Soak(args) = parse_args([
            "soak",
            "--env=kube",
            "--scenario=kill",
            "--duration-secs=3600",
            "--kill-after-checkpoint=2",
            "--dump=/tmp/soak",
            "--prom-url=http://127.0.0.1:9090",
        ])
        .unwrap() else {
            panic!("expected soak");
        };
        assert_eq!(args.env, Some(RuntimeEnv::Kube));
        assert_eq!(
            args.scenario,
            Some(SoakScenario::KillAfterCheckpoint {
                after_checkpoint: 2
            })
        );
        assert_eq!(args.duration_secs, Some(3600));
        assert_eq!(args.dump.as_deref().unwrap().to_str().unwrap(), "/tmp/soak");
        assert_eq!(args.prom_url.as_deref(), Some("http://127.0.0.1:9090"));
    }

    #[test]
    fn parse_rejects_kill_flag_on_steady() {
        assert!(parse_args(["soak", "--kill-after-checkpoint", "1"]).is_err());
    }

    #[test]
    fn config_file_then_cli_override() {
        let path = std::env::temp_dir().join("volga-soak-config-test.yaml");
        std::fs::write(
            &path,
            r#"
env: docker
duration: 99s
scenario: kill
kill_after_checkpoint: 1
launch:
  sql: SELECT timestamp, key, value FROM datagen_source
  checkpoint:
    interval: 30s
"#,
        )
        .unwrap();
        let LongrunCommand::Soak(args) = parse_args([
            "soak",
            &format!("--config={}", path.display()),
            "--env=kube",
            "--duration-secs=3600",
        ])
        .unwrap() else {
            panic!("expected soak");
        };
        let spec = build_spec(args).unwrap();
        assert_eq!(spec.env, RuntimeEnv::Kube);
        assert_eq!(spec.duration, Duration::from_secs(3600));
        assert_eq!(
            spec.scenario,
            SoakScenario::KillAfterCheckpoint {
                after_checkpoint: 1
            }
        );
        let _ = std::fs::remove_file(path);
    }
}
