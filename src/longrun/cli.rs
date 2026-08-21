use std::time::Duration;

use anyhow::{anyhow, bail, Result};

use crate::test_utils::harness::RuntimeEnv;

use super::config::{apply_file, SoakFile};
use super::run::run_soak;
use super::spec::SoakSpec;

const USAGE: &str = "\
Usage:
  volga-longrun soak [--config FILE] [--env local|kube|docker] [--duration-secs N]
  volga-longrun bench

v1 implements soak only. Default duration: local 120s, kube/docker 3600s.
YAML (`--config`) is the soak spec: scenario, dump, Prom, oracles, job overlay.
Count sink and Datagen source are fixed. CLI only overrides env and duration.
Omit `--config` for the default window job (steady, local).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongrunCommand {
    Soak(SoakArgs),
    Bench,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoakArgs {
    pub config: Option<std::path::PathBuf>,
    pub env: Option<RuntimeEnv>,
    pub duration_secs: Option<u64>,
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
    let mut duration_secs = None;
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
            other => bail!("unknown soak flag `{other}`\n{USAGE}"),
        }
        i += 1;
    }
    Ok(SoakArgs {
        config,
        env,
        duration_secs,
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
    use crate::longrun::spec::SoakScenario;

    #[test]
    fn parse_soak_env_duration() {
        let LongrunCommand::Soak(args) = parse_args([
            "soak",
            "--env=kube",
            "--duration-secs=3600",
        ])
        .unwrap() else {
            panic!("expected soak");
        };
        assert_eq!(args.env, Some(RuntimeEnv::Kube));
        assert_eq!(args.duration_secs, Some(3600));
        assert!(args.config.is_none());
    }

    #[test]
    fn parse_rejects_scenario_flags() {
        assert!(parse_args(["soak", "--scenario", "kill"]).is_err());
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
