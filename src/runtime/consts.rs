use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

pub const MASTER_DISCOVERY_TIMEOUT: &str = "master.discovery_timeout";
pub const MASTER_REPLACEMENT_TIMEOUT: &str = "master.replacement_timeout";
pub const MASTER_RECOVERY_BUDGET: &str = "master.recovery_budget";
pub const MASTER_RPC_MAX_RETRIES: &str = "master.rpc_max_retries";
pub const MASTER_RPC_RETRY_DELAY: &str = "master.rpc_retry_delay";
/// After the first fatal during an attempt, wait this long for cascade fatals
/// before grouping by worker and deciding replace vs reuse.
pub const MASTER_FAILURE_AGGREGATION_WINDOW: &str = "master.failure_aggregation_window";
pub const MASTER_HEARTBEAT_MAX_STREAM_ATTEMPTS: &str = "master.heartbeat_max_stream_attempts";
pub const MASTER_HEARTBEAT_RECONNECT_DELAY: &str = "master.heartbeat_reconnect_delay";
/// TCP/HTTP connect timeout for master → worker control dials (HB re-dial, open, channel reconnect).
pub const MASTER_WORKER_CONNECT_TIMEOUT: &str = "master.worker_connect_timeout";
/// How often `run()` polls `get_worker_state` (completion + unreachable detection).
pub const MASTER_STATE_POLL_INTERVAL: &str = "master.state_poll_interval";
pub const TRANSPORT_GRPC_CONNECT_MAX_RETRIES: &str = "transport.grpc_connect_max_retries";
pub const TRANSPORT_GRPC_CONNECT_RETRY_DELAY: &str = "transport.grpc_connect_retry_delay";

/// When set (`1`/`true`/`yes`), load `runtime_consts.test.json` instead of production.
pub const VOLGA_USE_TEST_CONSTS_ENV: &str = "VOLGA_USE_TEST_CONSTS";
/// Explicit path to a consts JSON file (overrides profile selection).
pub const VOLGA_RUNTIME_CONSTS_PATH_ENV: &str = "VOLGA_RUNTIME_CONSTS_PATH";
/// Directory containing `runtime_consts.{production,test}.json`.
pub const VOLGA_RUNTIME_CONSTS_DIR_ENV: &str = "VOLGA_RUNTIME_CONSTS_DIR";

const DURATION_KEYS: &[&str] = &[
    MASTER_DISCOVERY_TIMEOUT,
    MASTER_REPLACEMENT_TIMEOUT,
    MASTER_RPC_RETRY_DELAY,
    MASTER_FAILURE_AGGREGATION_WINDOW,
    MASTER_HEARTBEAT_RECONNECT_DELAY,
    MASTER_WORKER_CONNECT_TIMEOUT,
    MASTER_STATE_POLL_INTERVAL,
    TRANSPORT_GRPC_CONNECT_RETRY_DELAY,
];

const U64_KEYS: &[&str] = &[
    MASTER_RECOVERY_BUDGET,
    MASTER_RPC_MAX_RETRIES,
    MASTER_HEARTBEAT_MAX_STREAM_ATTEMPTS,
    TRANSPORT_GRPC_CONNECT_MAX_RETRIES,
];

const EMBEDDED_PRODUCTION: &str =
    include_str!("../../config/runtime_consts.production.json");
const EMBEDDED_TEST: &str = include_str!("../../config/runtime_consts.test.json");

#[derive(Clone, Debug)]
pub enum RuntimeValue {
    Duration(Duration),
    U64(u64),
}

#[derive(Clone, Debug)]
pub struct RuntimeConsts {
    values: HashMap<String, RuntimeValue>,
}

impl RuntimeConsts {
    pub fn production() -> Self {
        load_profile(false)
    }

    pub fn test() -> Self {
        load_profile(true)
    }

    pub fn duration(&self, key: &'static str) -> Duration {
        match self.values.get(key) {
            Some(RuntimeValue::Duration(value)) => *value,
            _ => panic!("runtime constant {key} is missing or not a duration"),
        }
    }

    pub fn u64(&self, key: &'static str) -> u64 {
        match self.values.get(key) {
            Some(RuntimeValue::U64(value)) => *value,
            _ => panic!("runtime constant {key} is missing or not an integer"),
        }
    }
}

fn load_profile(test: bool) -> RuntimeConsts {
    let name = if test {
        "runtime_consts.test.json"
    } else {
        "runtime_consts.production.json"
    };
    if let Some(path) = resolve_consts_path(test) {
        match fs::read_to_string(&path) {
            Ok(raw) => {
                println!("[MASTER] loaded runtime consts from {}", path.display());
                return parse_consts_json(&raw);
            }
            Err(err) => {
                eprintln!(
                    "[MASTER] failed to read runtime consts {}: {err}; falling back to embedded",
                    path.display()
                );
            }
        }
    }
    println!("[MASTER] using embedded runtime consts ({name})");
    parse_consts_json(if test { EMBEDDED_TEST } else { EMBEDDED_PRODUCTION })
}

fn resolve_consts_path(test: bool) -> Option<PathBuf> {
    if let Ok(path) = env::var(VOLGA_RUNTIME_CONSTS_PATH_ENV) {
        return Some(PathBuf::from(path));
    }
    let name = if test {
        "runtime_consts.test.json"
    } else {
        "runtime_consts.production.json"
    };
    let mut candidates = Vec::new();
    if let Ok(dir) = env::var(VOLGA_RUNTIME_CONSTS_DIR_ENV) {
        candidates.push(PathBuf::from(dir).join(name));
    }
    candidates.push(PathBuf::from("/etc/volga").join(name));
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join(name),
    );
    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join("config").join(name));
    }
    candidates.into_iter().find(|p| p.is_file())
}

fn parse_consts_json(raw: &str) -> RuntimeConsts {
    let map: HashMap<String, serde_json::Value> = serde_json::from_str(raw)
        .unwrap_or_else(|e| panic!("invalid runtime consts JSON: {e}"));
    let mut values = HashMap::new();
    for &key in DURATION_KEYS {
        let value = map
            .get(key)
            .unwrap_or_else(|| panic!("runtime consts missing duration key {key}"));
        values.insert(key.to_string(), RuntimeValue::Duration(json_duration(key, value)));
    }
    for &key in U64_KEYS {
        let value = map
            .get(key)
            .unwrap_or_else(|| panic!("runtime consts missing integer key {key}"));
        values.insert(key.to_string(), RuntimeValue::U64(json_u64(key, value)));
    }
    RuntimeConsts { values }
}

fn json_duration(key: &str, value: &serde_json::Value) -> Duration {
    match value {
        serde_json::Value::String(s) => parse_duration(s)
            .unwrap_or_else(|e| panic!("runtime consts {key}: {e}")),
        serde_json::Value::Number(n) => {
            let ms = n
                .as_u64()
                .unwrap_or_else(|| panic!("runtime consts {key}: expected non-negative integer ms"));
            Duration::from_millis(ms)
        }
        _ => panic!("runtime consts {key}: expected duration string or integer ms"),
    }
}

fn json_u64(key: &str, value: &serde_json::Value) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("runtime consts {key}: expected non-negative integer"))
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix("ms") {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration '{s}'"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(rest) = s.strip_suffix("us") {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration '{s}'"))?;
        return Ok(Duration::from_micros(n));
    }
    if let Some(rest) = s.strip_suffix('s') {
        let n: u64 = rest
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration '{s}'"))?;
        return Ok(Duration::from_secs(n));
    }
    Err(format!(
        "invalid duration '{s}' (use Ns, Nms, or Nus)"
    ))
}

static RUNTIME_CONSTS: OnceLock<RuntimeConsts> = OnceLock::new();

fn env_flag_enabled(name: &str) -> bool {
    matches!(
        env::var(name).as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn select_runtime_consts() -> RuntimeConsts {
    if cfg!(test) || env_flag_enabled(VOLGA_USE_TEST_CONSTS_ENV) {
        println!("[MASTER] using test runtime consts");
        RuntimeConsts::test()
    } else {
        RuntimeConsts::production()
    }
}

/// Install consts before first use (e.g. after reading a kube annotation). No-op if already set.
pub fn init_runtime_consts(consts: RuntimeConsts) {
    let _ = RUNTIME_CONSTS.set(consts);
}

/// Force test consts from harness/annotation when env was not set on the pod.
pub fn init_test_runtime_consts() {
    init_runtime_consts(RuntimeConsts::test());
    println!("[MASTER] using test runtime consts (explicit init)");
}

pub fn runtime_consts() -> &'static RuntimeConsts {
    RUNTIME_CONSTS.get_or_init(select_runtime_consts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_embedded_profiles() {
        let prod = parse_consts_json(EMBEDDED_PRODUCTION);
        assert_eq!(prod.duration(MASTER_DISCOVERY_TIMEOUT), Duration::from_secs(30));
        assert_eq!(prod.u64(MASTER_RECOVERY_BUDGET), 20);

        let test = parse_consts_json(EMBEDDED_TEST);
        assert_eq!(test.duration(MASTER_DISCOVERY_TIMEOUT), Duration::from_secs(2));
        assert_eq!(
            test.duration(MASTER_WORKER_CONNECT_TIMEOUT),
            Duration::from_millis(200)
        );
        assert_eq!(test.u64(MASTER_RECOVERY_BUDGET), 5);
    }

    #[test]
    fn resolve_finds_repo_config() {
        let path = resolve_consts_path(true).expect("test consts file");
        assert!(path.ends_with("runtime_consts.test.json"));
        assert!(Path::new(&path).is_file());
    }
}
