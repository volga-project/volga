use std::time::Duration;

use anyhow::{anyhow, bail, Result};

use crate::test_utils::harness::RuntimeEnv;

use super::run::run_soak;
use super::spec::{SoakScenario, SoakSpec};

const USAGE: &str = "\
Usage:
  volga-longrun soak [--env local|kube|docker] [--scenario steady|kill] [--duration-secs N]
                     [--kill-after-checkpoint N] [--dump DIR] [--prom-url URL]
  volga-longrun bench

v1 implements soak only. Default duration: local 120s, kube/docker 3600s.
Checkpoints use prod consts (30s interval). Count sink. Sliding RANGE window job.
--dump writes lifecycle.json (and prom.json when --prom-url is set).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongrunCommand {
    Soak(SoakArgs),
    Bench,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoakArgs {
    pub env: RuntimeEnv,
    pub scenario: SoakScenario,
    pub duration_secs: Option<u64>,
    pub dump: Option<std::path::PathBuf>,
    pub prom_url: Option<String>,
}

pub async fn run<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    match parse_args(args)? {
        LongrunCommand::Soak(args) => {
            let duration_secs = args
                .duration_secs
                .unwrap_or_else(|| SoakSpec::default_duration_secs(args.env));
            let spec = SoakSpec {
                dump: args.dump,
                prom_url: args.prom_url,
                ..SoakSpec::new(
                    args.env,
                    args.scenario,
                    Duration::from_secs(duration_secs),
                )
            };
            run_soak(spec).await
        }
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
    let mut env = RuntimeEnv::Local;
    let mut scenario_kill = false;
    let mut duration_secs = None;
    let mut kill_after = 1u64;
    let mut kill_after_set = false;
    let mut dump = None;
    let mut prom_url = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let (flag, inline) = split_flag(arg);
        match flag {
            "--env" => {
                let value = take_value(flag, inline, args, &mut i)?;
                env = RuntimeEnv::parse(&value)
                    .ok_or_else(|| anyhow!("invalid --env `{value}` (local|kube|docker)"))?;
            }
            "--scenario" => {
                let value = take_value(flag, inline, args, &mut i)?;
                scenario_kill = match value.as_str() {
                    "steady" => false,
                    "kill" => true,
                    other => bail!("invalid --scenario `{other}` (steady|kill)"),
                };
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
                kill_after = n;
                kill_after_set = true;
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
    if kill_after_set && !scenario_kill {
        bail!("--kill-after-checkpoint requires --scenario kill");
    }
    let scenario = if scenario_kill {
        SoakScenario::KillAfterCheckpoint {
            after_checkpoint: kill_after,
        }
    } else {
        SoakScenario::Steady
    };
    Ok(SoakArgs {
        env,
        scenario,
        duration_secs,
        dump,
        prom_url,
    })
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
        assert_eq!(args.env, RuntimeEnv::Kube);
        assert_eq!(
            args.scenario,
            SoakScenario::KillAfterCheckpoint {
                after_checkpoint: 2
            }
        );
        assert_eq!(args.duration_secs, Some(3600));
        assert_eq!(args.dump.as_deref().unwrap().to_str().unwrap(), "/tmp/soak");
        assert_eq!(args.prom_url.as_deref(), Some("http://127.0.0.1:9090"));
    }

    #[test]
    fn parse_rejects_kill_flag_on_steady() {
        assert!(parse_args(["soak", "--kill-after-checkpoint", "1"]).is_err());
    }
}
