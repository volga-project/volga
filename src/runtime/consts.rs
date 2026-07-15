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
/// How often master sends a heartbeat tick on an open control stream.
pub const MASTER_HEARTBEAT_SEND_INTERVAL: &str = "master.heartbeat_send_interval";
/// TCP/HTTP connect timeout for master → worker control dials (HB re-dial, open, channel reconnect).
pub const MASTER_WORKER_CONNECT_TIMEOUT: &str = "master.worker_connect_timeout";
/// How often `run()` polls `get_worker_state` (completion + unreachable detection).
pub const MASTER_STATE_POLL_INTERVAL: &str = "master.state_poll_interval";
/// Bound on `reset_worker` RPCs during attempt teardown (failed → expand replace set).
pub const MASTER_RESET_WORKER_TIMEOUT: &str = "master.reset_worker_timeout";
/// Sleep between worker-registry readiness polls (discovery / replacement wait).
pub const MASTER_REGISTRY_WAIT_TICK: &str = "master.registry_wait_tick";
pub const KUBE_WORKER_HEALTH_POLL_INTERVAL: &str = "kube.worker_health_poll_interval";
pub const KUBE_WORKER_HEALTH_UNHEALTHY_GRACE_TICKS: &str =
    "kube.worker_health_unhealthy_grace_ticks";
pub const WORKER_HEARTBEAT_SEND_INTERVAL: &str = "worker.heartbeat_send_interval";
pub const WORKER_HEARTBEAT_MASTER_SILENCE_TIMEOUT: &str =
    "worker.heartbeat_master_silence_timeout";
pub const WORKER_REGISTER_MAX_RETRIES: &str = "worker.register_max_retries";
pub const WORKER_REGISTER_RETRY_DELAY: &str = "worker.register_retry_delay";
pub const WORKER_REGISTER_CONNECT_TIMEOUT: &str = "worker.register_connect_timeout";
pub const WORKER_REGISTER_RPC_TIMEOUT: &str = "worker.register_rpc_timeout";
pub const TRANSPORT_GRPC_CONNECT_MAX_RETRIES: &str = "transport.grpc_connect_max_retries";
pub const TRANSPORT_GRPC_CONNECT_RETRY_DELAY: &str = "transport.grpc_connect_retry_delay";

/// Explicit profile: `local_test` | `kube_test` | `prod`.
pub const VOLGA_RUNTIME_CONSTS_PROFILE_ENV: &str = "VOLGA_RUNTIME_CONSTS_PROFILE";
/// Deprecated alias for profile=`local_test` (`1`/`true`/`yes`).
pub const VOLGA_USE_TEST_CONSTS_ENV: &str = "VOLGA_USE_TEST_CONSTS";
/// Explicit path to a consts JSON file (overrides profile selection).
pub const VOLGA_RUNTIME_CONSTS_PATH_ENV: &str = "VOLGA_RUNTIME_CONSTS_PATH";
/// Directory containing `runtime_consts.{local_test,kube_test,prod}.json`.
pub const VOLGA_RUNTIME_CONSTS_DIR_ENV: &str = "VOLGA_RUNTIME_CONSTS_DIR";

const DURATION_KEYS: &[&str] = &[
    MASTER_DISCOVERY_TIMEOUT,
    MASTER_REPLACEMENT_TIMEOUT,
    MASTER_RPC_RETRY_DELAY,
    MASTER_FAILURE_AGGREGATION_WINDOW,
    MASTER_HEARTBEAT_RECONNECT_DELAY,
    MASTER_HEARTBEAT_SEND_INTERVAL,
    MASTER_WORKER_CONNECT_TIMEOUT,
    MASTER_STATE_POLL_INTERVAL,
    MASTER_RESET_WORKER_TIMEOUT,
    MASTER_REGISTRY_WAIT_TICK,
    KUBE_WORKER_HEALTH_POLL_INTERVAL,
    WORKER_HEARTBEAT_SEND_INTERVAL,
    WORKER_HEARTBEAT_MASTER_SILENCE_TIMEOUT,
    WORKER_REGISTER_RETRY_DELAY,
    WORKER_REGISTER_CONNECT_TIMEOUT,
    WORKER_REGISTER_RPC_TIMEOUT,
    TRANSPORT_GRPC_CONNECT_RETRY_DELAY,
];

const U64_KEYS: &[&str] = &[
    MASTER_RECOVERY_BUDGET,
    MASTER_RPC_MAX_RETRIES,
    MASTER_HEARTBEAT_MAX_STREAM_ATTEMPTS,
    KUBE_WORKER_HEALTH_UNHEALTHY_GRACE_TICKS,
    WORKER_REGISTER_MAX_RETRIES,
    TRANSPORT_GRPC_CONNECT_MAX_RETRIES,
];

const EMBEDDED_LOCAL_TEST: &str = include_str!("../../config/runtime_consts.local_test.json");
const EMBEDDED_KUBE_TEST: &str = include_str!("../../config/runtime_consts.kube_test.json");
const EMBEDDED_PROD: &str = include_str!("../../config/runtime_consts.prod.json");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeConstsProfile {
    /// Fast budgets for in-process local tests.
    LocalTest,
    /// Cluster budgets (STS ready / register); used by kube harness.
    KubeTest,
    /// Default for non-test binaries. Same values as [`Self::KubeTest`].
    Prod,
}

impl RuntimeConstsProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalTest => "local_test",
            Self::KubeTest => "kube_test",
            Self::Prod => "prod",
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            Self::LocalTest => "runtime_consts.local_test.json",
            Self::KubeTest => "runtime_consts.kube_test.json",
            Self::Prod => "runtime_consts.prod.json",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local_test" | "local" | "test" => Some(Self::LocalTest),
            "kube_test" | "kube" | "kubernetes" => Some(Self::KubeTest),
            "prod" | "production" => Some(Self::Prod),
            _ => None,
        }
    }
}

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
    pub fn for_profile(profile: RuntimeConstsProfile) -> Self {
        load_profile(profile)
    }

    pub fn local_test() -> Self {
        Self::for_profile(RuntimeConstsProfile::LocalTest)
    }

    pub fn kube_test() -> Self {
        Self::for_profile(RuntimeConstsProfile::KubeTest)
    }

    pub fn prod() -> Self {
        Self::for_profile(RuntimeConstsProfile::Prod)
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

fn load_profile(profile: RuntimeConstsProfile) -> RuntimeConsts {
    let name = profile.file_name();
    if let Some(path) = resolve_consts_path(profile) {
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
    parse_consts_json(match profile {
        RuntimeConstsProfile::LocalTest => EMBEDDED_LOCAL_TEST,
        RuntimeConstsProfile::KubeTest => EMBEDDED_KUBE_TEST,
        RuntimeConstsProfile::Prod => EMBEDDED_PROD,
    })
}

fn resolve_consts_path(profile: RuntimeConstsProfile) -> Option<PathBuf> {
    if let Ok(path) = env::var(VOLGA_RUNTIME_CONSTS_PATH_ENV) {
        return Some(PathBuf::from(path));
    }
    let name = profile.file_name();
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

fn profile_from_env() -> Option<RuntimeConstsProfile> {
    if let Ok(raw) = env::var(VOLGA_RUNTIME_CONSTS_PROFILE_ENV) {
        return RuntimeConstsProfile::parse(&raw);
    }
    if env_flag_enabled(VOLGA_USE_TEST_CONSTS_ENV) {
        return Some(RuntimeConstsProfile::LocalTest);
    }
    None
}

fn select_runtime_consts() -> RuntimeConsts {
    let profile = profile_from_env().unwrap_or_else(|| {
        if cfg!(test) {
            RuntimeConstsProfile::LocalTest
        } else {
            RuntimeConstsProfile::Prod
        }
    });
    println!(
        "[MASTER] using {} runtime consts",
        profile.as_str()
    );
    RuntimeConsts::for_profile(profile)
}

/// Install consts before first use (e.g. after reading a kube annotation). No-op if already set.
pub fn init_runtime_consts(consts: RuntimeConsts) {
    let _ = RUNTIME_CONSTS.set(consts);
}

pub fn init_runtime_consts_profile(profile: RuntimeConstsProfile) {
    println!(
        "[MASTER] using {} runtime consts (explicit init)",
        profile.as_str()
    );
    init_runtime_consts(RuntimeConsts::for_profile(profile));
}

/// Force local_test consts from harness/annotation when env was not set on the pod.
pub fn init_local_test_runtime_consts() {
    init_runtime_consts_profile(RuntimeConstsProfile::LocalTest);
}

/// Deprecated name for [`init_local_test_runtime_consts`].
pub fn init_local_runtime_consts() {
    init_local_test_runtime_consts();
}

/// Deprecated name for [`init_local_test_runtime_consts`].
pub fn init_test_runtime_consts() {
    init_local_test_runtime_consts();
}

pub fn runtime_consts() -> &'static RuntimeConsts {
    RUNTIME_CONSTS.get_or_init(select_runtime_consts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_embedded_profiles() {
        let prod = parse_consts_json(EMBEDDED_PROD);
        assert_eq!(prod.duration(MASTER_DISCOVERY_TIMEOUT), Duration::from_secs(30));
        assert_eq!(prod.u64(MASTER_RECOVERY_BUDGET), 20);
        assert_eq!(
            prod.duration(MASTER_FAILURE_AGGREGATION_WINDOW),
            Duration::from_secs(3)
        );

        let kube = parse_consts_json(EMBEDDED_KUBE_TEST);
        assert_eq!(
            kube.duration(KUBE_WORKER_HEALTH_POLL_INTERVAL),
            Duration::from_millis(500)
        );
        assert_eq!(kube.u64(KUBE_WORKER_HEALTH_UNHEALTHY_GRACE_TICKS), 2);

        let local = parse_consts_json(EMBEDDED_LOCAL_TEST);
        assert_eq!(local.duration(MASTER_DISCOVERY_TIMEOUT), Duration::from_secs(2));
        assert_eq!(
            local.duration(MASTER_WORKER_CONNECT_TIMEOUT),
            Duration::from_millis(200)
        );
        assert_eq!(local.u64(MASTER_RECOVERY_BUDGET), 5);
    }

    #[test]
    fn kube_test_matches_prod() {
        let kube = parse_consts_json(EMBEDDED_KUBE_TEST);
        let prod = parse_consts_json(EMBEDDED_PROD);
        for &key in DURATION_KEYS {
            assert_eq!(
                kube.duration(key),
                prod.duration(key),
                "duration mismatch for {key}"
            );
        }
        for &key in U64_KEYS {
            assert_eq!(kube.u64(key), prod.u64(key), "u64 mismatch for {key}");
        }
    }

    #[test]
    fn resolve_finds_repo_config() {
        let path =
            resolve_consts_path(RuntimeConstsProfile::LocalTest).expect("local_test consts file");
        assert!(path.ends_with("runtime_consts.local_test.json"));
        assert!(Path::new(&path).is_file());
    }
}
