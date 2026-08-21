use std::time::Duration;

use anyhow::{anyhow, bail, Result};

use crate::test_utils::harness::RuntimeEnv;

use super::config::{apply_file, BenchFile};
use super::run::run_bench;
use super::spec::BenchSpec;

const USAGE: &str = "\
Usage:
  volga-bench [--config FILE] [--env local|kube|docker] [--duration-secs N]

YAML (`--config`) is the run spec: scenario, dump, Prom, oracles, job overlay.
Count sink and Datagen source are fixed. CLI only overrides env and duration.
Omit `--config` for the default window job (steady, local, 120s).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub config: Option<std::path::PathBuf>,
    pub env: Option<RuntimeEnv>,
    pub duration_secs: Option<u64>,
}

pub async fn run<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    run_bench(build_spec(parse_args(args)?)?).await
}

pub fn parse_args<I, S>(args: I) -> Result<Args>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        bail!("{USAGE}");
    }
    let mut config = None;
    let mut env = None;
    let mut duration_secs = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let (flag, inline) = split_flag(arg);
        match flag {
            "--config" => {
                config = Some(std::path::PathBuf::from(take_value(flag, inline, &args, &mut i)?));
            }
            "--env" => {
                let value = take_value(flag, inline, &args, &mut i)?;
                env = Some(RuntimeEnv::parse(&value).ok_or_else(|| {
                    anyhow!("invalid --env `{value}` (local|kube|docker)")
                })?);
            }
            "--duration-secs" => {
                let value = take_value(flag, inline, &args, &mut i)?;
                let n: u64 = value
                    .parse()
                    .map_err(|_| anyhow!("invalid --duration-secs `{value}`"))?;
                if n == 0 {
                    bail!("--duration-secs must be >= 1");
                }
                duration_secs = Some(n);
            }
            other => bail!("unknown flag `{other}`\n{USAGE}"),
        }
        i += 1;
    }
    Ok(Args {
        config,
        env,
        duration_secs,
    })
}

fn build_spec(args: Args) -> Result<BenchSpec> {
    let file = match &args.config {
        Some(path) => BenchFile::load(path)?,
        None => BenchFile::default(),
    };
    let mut spec = apply_file(&file)?;
    if let Some(env) = args.env {
        spec.env = env;
    }
    if let Some(secs) = args.duration_secs {
        spec.duration = Duration::from_secs(secs);
    } else if file.duration.is_none() {
        spec.duration = Duration::from_secs(BenchSpec::default_duration_secs(spec.env));
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
    use crate::bench::spec::Scenario;

    #[test]
    fn config_file_then_cli_override() {
        let path = std::env::temp_dir().join("volga-bench-config-test.yaml");
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
        let args = parse_args([
            &format!("--config={}", path.display()),
            "--env=kube",
            "--duration-secs=3600",
        ])
        .unwrap();
        let spec = build_spec(args).unwrap();
        assert_eq!(spec.env, RuntimeEnv::Kube);
        assert_eq!(spec.duration, Duration::from_secs(3600));
        assert_eq!(
            spec.scenario,
            Scenario::KillAfterCheckpoint {
                after_checkpoint: 1
            }
        );
        let _ = std::fs::remove_file(path);
    }
}
